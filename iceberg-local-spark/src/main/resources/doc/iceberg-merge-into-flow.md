# Apache Iceberg MERGE INTO 流程详解

> 基于 Apache Iceberg 1.7.0-SNAPSHOT 和 Spark 3.4.3 源码分析
>
> 文档目标：深入理解 MERGE INTO 的执行机制，对比 Copy-on-Write 和 Merge-on-Read 两种模式

---

## 1. MERGE INTO 概述

### 1.1 SQL 语法

```sql
MERGE INTO target_table AS target
USING source_table AS source
ON target.id = source.id AND target.dt = source.dt
WHEN MATCHED THEN
  UPDATE SET target.name = source.name, target.age = source.age
WHEN NOT MATCHED THEN
  INSERT (id, name, age, dt) VALUES (source.id, source.name, source.age, source.dt)
```

### 1.2 语义说明

```
MERGE INTO 执行三种操作：

1. WHEN MATCHED THEN UPDATE
   ↓
   当 target 和 source 的 JOIN 条件匹配时，更新 target 的行

2. WHEN MATCHED THEN DELETE
   ↓
   当 target 和 source 的 JOIN 条件匹配时，删除 target 的行

3. WHEN NOT MATCHED THEN INSERT
   ↓
   当 source 的行在 target 中找不到匹配时，插入到 target

可选的 WHEN NOT MATCHED BY SOURCE：
   ↓
   当 target 的行在 source 中找不到匹配时的处理（Iceberg 暂不支持）
```

---

## 2. MERGE INTO 执行架构

### 2.1 执行流程概览

```
┌──────────────────────────────────────────────────────────────────┐
│                   MERGE INTO 执行流程                              │
└──────────────────────────────────────────────────────────────────┘

Spark SQL 解析
    ↓
┌────────────────────────────────┐
│  1. 逻辑计划（Logical Plan）    │
│     MergeIntoIcebergTable      │
└────────────────────────────────┘
    ↓
┌────────────────────────────────┐
│  2. 分析与改写                  │
│     RewriteMergeIntoTable      │
│     ↓                          │
│     决定执行模式：               │
│     - ReplaceIcebergData (CoW) │
│     - WriteIcebergDelta (MoR)  │
└────────────────────────────────┘
    ↓
┌────────────────────────────────┐
│  3. 物理计划（Physical Plan）   │
│     MergeRowsExec              │
│     ↓                          │
│     执行 JOIN + 合并逻辑        │
└────────────────────────────────┘
    ↓
┌────────────────────────────────┐
│  4. 数据写入                    │
│     - CoW: OverwriteFiles      │
│     - MoR: RowDelta            │
└────────────────────────────────┘
    ↓
新 Snapshot 生成
```

### 2.2 核心类与文件

| 组件 | 文件路径 | 作用 |
|-----|---------|------|
| **MergeIntoIcebergTable** | `spark-extensions/.../MergeIntoIcebergTable.scala` | 逻辑计划节点 |
| **RewriteMergeIntoTable** | `spark-extensions/.../RewriteMergeIntoTable.scala` | 改写规则 |
| **MergeRowsExec** | `spark-extensions/.../MergeRowsExec.scala` | 物理执行 |
| **ReplaceIcebergData** | `spark-extensions/.../ReplaceIcebergData.scala` | CoW 模式写入 |
| **WriteIcebergDelta** | `spark-extensions/.../WriteIcebergDelta.scala` | MoR 模式写入 |

---

## 3. RewriteMergeIntoTable：改写逻辑

### 3.1 源码位置

**文件**: `spark/v3.4/spark-extensions/src/main/scala/org/apache/spark/sql/catalyst/analysis/RewriteMergeIntoTable.scala`

### 3.2 改写策略

```scala
object RewriteMergeIntoTable extends RewriteRowLevelIcebergCommand {

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case m @ MergeIntoIcebergTable(aliasedTable, source, cond, matchedActions, notMatchedActions, None)
        if m.resolved && m.aligned =>

      EliminateSubqueryAliases(aliasedTable) match {
        case r @ DataSourceV2Relation(tbl: SupportsRowLevelOperations, _, _, _, _) =>
          val table = buildOperationTable(tbl, MERGE, CaseInsensitiveStringMap.empty())

          // 根据表的能力选择执行模式
          val rewritePlan = table.operation match {
            case _: SupportsDelta =>
              // 支持 RowDelta → 使用 MoR 模式
              buildWriteDeltaPlan(r, table, source, cond, matchedActions, notMatchedActions)

            case _ =>
              // 不支持 RowDelta → 使用 CoW 模式
              buildReplaceDataPlan(r, table, source, cond, matchedActions, notMatchedActions)
          }

          m.copy(rewritePlan = Some(rewritePlan))
      }
  }
}
```

### 3.3 决策逻辑

```
如何决定使用 CoW 还是 MoR？

┌──────────────────────────────────────────────────────┐
│  检查表是否实现 SupportsDelta 接口                      │
└──────────────────────────────────────────────────────┘
             ↓                          ↓
    ✅ 是（实现了）             ❌ 否（未实现）
             ↓                          ↓
┌────────────────────────┐    ┌──────────────────────────┐
│  buildWriteDeltaPlan   │    │  buildReplaceDataPlan    │
│  (MoR 模式)             │    │  (CoW 模式)               │
│                        │    │                          │
│  - 使用 RowDelta API   │    │  - 使用 OverwriteFiles   │
│  - 生成 Delete Files   │    │  - 重写数据文件           │
│  - 写入性能高          │    │  - 读取性能高             │
└────────────────────────┘    └──────────────────────────┘

配置影响：
  write.merge.mode=merge-on-read  → SupportsDelta = true
  write.merge.mode=copy-on-write  → SupportsDelta = false
```

---

## 4. Copy-on-Write 模式详解

### 4.1 执行流程

```
┌──────────────────────────────────────────────────────────────────┐
│                 Copy-on-Write 模式执行流程                          │
└──────────────────────────────────────────────────────────────────┘

示例：
  MERGE INTO users AS target
  USING updates AS source
  ON target.id = source.id
  WHEN MATCHED THEN UPDATE SET *
  WHEN NOT MATCHED THEN INSERT *

┌────────────────────────────────┐
│  1. 扫描 Target 表              │
│     读取所有受影响的数据文件       │
└────────────────────────────────┘
    ↓
    读取分区: dt=2024-10-26
    ↓
    Files: [f1.parquet (100MB, 1000 rows),
            f2.parquet (100MB, 1000 rows)]
    ↓
┌────────────────────────────────┐
│  2. 构建 JOIN 计划              │
│     Target LEFT/FULL OUTER     │
│            JOIN Source         │
└────────────────────────────────┘
    ↓
    使用 FULL OUTER JOIN（因为有 NOT MATCHED）
    ↓
    JOIN 结果：
      - Matched rows: 500 条（需要 UPDATE）
      - Target-only rows: 1500 条（保持不变）
      - Source-only rows: 100 条（需要 INSERT）
    ↓
┌────────────────────────────────┐
│  3. MergeRows 操作              │
│     合并逻辑处理                 │
└────────────────────────────────┘
    ↓
    对每一行判断：
      - isSourceRowPresent && isTargetRowPresent
        → MATCHED: 应用 UPDATE
      - !isSourceRowPresent && isTargetRowPresent
        → Target-only: 保持原行
      - isSourceRowPresent && !isTargetRowPresent
        → NOT MATCHED: 应用 INSERT
    ↓
    输出：2100 rows（1500 + 500 + 100）
    ↓
┌────────────────────────────────┐
│  4. 重写数据文件                 │
│     生成新文件，删除旧文件         │
└────────────────────────────────┘
    ↓
    写入新文件：
      f3.parquet (210MB, 2100 rows)
    ↓
┌────────────────────────────────┐
│  5. 提交 Snapshot               │
│     使用 OverwriteFiles API     │
└────────────────────────────────┘
    ↓
    overwrite
      .deleteFile(f1.parquet)
      .deleteFile(f2.parquet)
      .addFile(f3.parquet)
      .commit()
    ↓
    新 Snapshot 生成
      - deleted-data-files: 2
      - added-data-files: 1
      - operation: "overwrite"
```

### 4.2 buildReplaceDataPlan 源码分析

```scala
private def buildReplaceDataPlan(
    relation: DataSourceV2Relation,
    operationTable: RowLevelOperationTable,
    source: LogicalPlan,
    cond: Expression,
    matchedActions: Seq[MergeAction],
    notMatchedActions: Seq[MergeAction]): ReplaceIcebergData = {

  // 1. 解析所需的元数据列（如分区列、Iceberg 内部列）
  val metadataAttrs = resolveRequiredMetadataAttrs(relation, operationTable.operation)

  // 2. 构建读取计划，包含元数据列
  val readRelation = buildRelationWithAttrs(relation, operationTable, metadataAttrs)
  val readAttrs = readRelation.output

  // 3. 是否需要基数检查（Cardinality Check）
  //    防止一个 target 行匹配多个 source 行
  val performCardinalityCheck = isCardinalityCheckNeeded(matchedActions)

  // 4. 为 Target 表添加标记列 __row_from_target
  //    用于判断 JOIN 后该行是否来自 target
  val rowFromTarget = Alias(TrueLiteral, ROW_FROM_TARGET)()
  val targetTableProjExprs = if (performCardinalityCheck) {
    val rowId = Alias(MonotonicallyIncreasingID(), ROW_ID)()
    readAttrs ++ Seq(rowFromTarget, rowId)
  } else {
    readAttrs :+ rowFromTarget
  }
  val targetTableProj = Project(targetTableProjExprs, readRelation)

  // 5. 为 Source 表添加标记列 __row_from_source
  val rowFromSource = Alias(TrueLiteral, ROW_FROM_SOURCE)()
  val sourceTableProjExprs = source.output :+ rowFromSource
  val sourceTableProj = Project(sourceTableProjExprs, source)

  // 6. 构建 JOIN
  //    - 如果没有 NOT MATCHED 操作，使用 LEFT OUTER JOIN
  //    - 否则使用 FULL OUTER JOIN
  val joinType = if (notMatchedActions.isEmpty) LeftOuter else FullOuter
  val joinHint = JoinHint(leftHint = Some(HintInfo(Some(NO_BROADCAST_HASH))), rightHint = None)
  val joinPlan = Join(NoStatsUnaryNode(targetTableProj), sourceTableProj, joinType, Some(cond), joinHint)

  // 7. 构建 MergeRows 操作
  val matchedConditions = matchedActions.map(actionCondition)
  val matchedOutputs = matchedActions.map(matchedActionOutput(_, metadataAttrs))
  val notMatchedConditions = notMatchedActions.map(actionCondition)
  val notMatchedOutputs = notMatchedActions.map(notMatchedActionOutput(_, metadataAttrs))

  val rowFromSourceAttr = resolveAttrRef(ROW_FROM_SOURCE_REF, joinPlan)
  val rowFromTargetAttr = resolveAttrRef(ROW_FROM_TARGET_REF, joinPlan)

  val mergeRows = MergeRows(
    isSourceRowPresent = IsNotNull(rowFromSourceAttr),
    isTargetRowPresent = if (notMatchedActions.isEmpty) TrueLiteral else IsNotNull(rowFromTargetAttr),
    matchedConditions = matchedConditions,
    matchedOutputs = matchedOutputs,
    notMatchedConditions = notMatchedConditions,
    notMatchedOutputs = notMatchedOutputs,
    targetOutput = readAttrs,
    performCardinalityCheck = performCardinalityCheck,
    emitNotMatchedTargetRows = true,  // CoW 模式需要保留未匹配的 target 行
    output = buildMergeRowsOutput(matchedOutputs, notMatchedOutputs :+ readAttrs, readAttrs),
    joinPlan)

  // 8. 构建 ReplaceIcebergData 计划
  //    替换受影响分区的文件
  val writeRelation = relation.copy(table = operationTable)
  ReplaceIcebergData(writeRelation, mergeRows, relation)
}
```

### 4.3 CoW 模式特点

```
优点：
  ✅ 读取性能高
     - 无需合并 Delete Files
     - 数据文件直接可读

  ✅ 数据一致性好
     - 文件内容完全一致，无历史残留

  ✅ Compaction 简单
     - 不需要处理 Delete Files

缺点：
  ⚠️  写放大严重
     - 更新 1 条记录 → 重写整个文件（可能 100MB+）

  ⚠️  写入性能低
     - 需要读取旧文件 + 写入新文件
     - I/O 开销大

  ⚠️  并发写入能力弱
     - 写入时需要锁定整个文件

适用场景：
  📊 OLAP 批处理
  📊 定时 ETL 任务
  📊 读多写少的场景
```

---

## 5. Merge-on-Read 模式详解

### 5.1 执行流程

```
┌──────────────────────────────────────────────────────────────────┐
│                 Merge-on-Read 模式执行流程                          │
└──────────────────────────────────────────────────────────────────┘

示例：
  MERGE INTO users AS target
  USING updates AS source
  ON target.id = source.id
  WHEN MATCHED THEN UPDATE SET *
  WHEN NOT MATCHED THEN INSERT *

┌────────────────────────────────┐
│  1. 扫描 Target 表              │
│     读取受影响的分区              │
└────────────────────────────────┘
    ↓
    分区: dt=2024-10-26
    Files: [f1.parquet (100MB, 1000 rows)]
    ↓
┌────────────────────────────────┐
│  2. 分离 JOIN 条件              │
│     splitMergeCond()           │
└────────────────────────────────┘
    ↓
    ON target.dt = source.dt AND target.id = source.id
    ↓
    分离为：
      - targetCond: dt = '2024-10-26'
        → 下推到 target scan（分区裁剪）
      - joinCond: target.id = source.id
        → JOIN 条件
    ↓
┌────────────────────────────────┐
│  3. 构建 JOIN 计划              │
│     Target INNER/RIGHT OUTER   │
│            JOIN Source         │
└────────────────────────────────┘
    ↓
    使用 RIGHT OUTER JOIN（因为有 NOT MATCHED）
    ↓
    JOIN 结果：
      - Matched rows: 500 条（需要 UPDATE）
      - Source-only rows: 100 条（需要 INSERT）
    ↓
┌────────────────────────────────┐
│  4. MergeRows 操作              │
│     生成操作类型标记              │
└────────────────────────────────┘
    ↓
    对每一行标记操作类型：
      - MATCHED → operation_type = DELETE + INSERT
        ↓
        生成两行：
          Row1: { operation: DELETE, id: 123, ... }
          Row2: { operation: INSERT, id: 123, name: "new_name", ... }

      - NOT MATCHED → operation_type = INSERT
        ↓
        生成一行：
          Row: { operation: INSERT, id: 456, ... }
    ↓
    输出：1100 rows
      - 500 DELETE + 500 INSERT (UPDATE) + 100 INSERT (NOT MATCHED)
    ↓
┌────────────────────────────────┐
│  5. 分离 Delete 和 Insert 行    │
│     WriteDeltaProjections      │
└────────────────────────────────┘
    ↓
    根据 operation_type 分离：
      deleteProjection: 500 rows (DELETE)
      insertProjection: 600 rows (500 UPDATE + 100 NOT MATCHED)
    ↓
┌────────────────────────────────┐
│  6. 写入 Delete Files           │
│     Position Delete            │
└────────────────────────────────┘
    ↓
    生成 delete/d1.parquet:
      ┌─────────────────┬─────┐
      │ file_path       │ pos │
      ├─────────────────┼─────┤
      │ data/f1.parquet │ 10  │
      │ data/f1.parquet │ 25  │
      │ ...             │ ... │
      │ data/f1.parquet │ 999 │
      └─────────────────┴─────┘
      (500 rows to delete)
    ↓
┌────────────────────────────────┐
│  7. 写入新 Data Files           │
└────────────────────────────────┘
    ↓
    生成 data/f2.parquet:
      包含 600 rows（500 更新的行 + 100 新插入的行）
    ↓
┌────────────────────────────────┐
│  8. 提交 Snapshot               │
│     使用 RowDelta API           │
└────────────────────────────────┘
    ↓
    rowDelta
      .addDeletes(d1.parquet)
      .addRows(f2.parquet)
      .commit()
    ↓
    新 Snapshot 生成
      - added-data-files: 1
      - added-delete-files: 1
      - deleted-records: 500
      - operation: "overwrite"
```

### 5.2 buildWriteDeltaPlan 源码分析

```scala
private def buildWriteDeltaPlan(
    relation: DataSourceV2Relation,
    operationTable: RowLevelOperationTable,
    source: LogicalPlan,
    cond: Expression,
    matchedActions: Seq[MergeAction],
    notMatchedActions: Seq[MergeAction]): WriteIcebergDelta = {

  // 1. 解析所需的列
  val rowAttrs = relation.output
  val rowIdAttrs = resolveRowIdAttrs(relation, operationTable.operation)  // 用于 Position Delete
  val metadataAttrs = resolveRequiredMetadataAttrs(relation, operationTable.operation)

  // 2. 构建读取计划
  val readRelation = buildRelationWithAttrs(relation, operationTable, rowIdAttrs ++ metadataAttrs)
  val readAttrs = readRelation.output

  // 3. 分离 MERGE 条件
  //    targetCond 可以下推到 target scan
  //    joinCond 用于 JOIN
  val (targetCond, joinCond) = splitMergeCond(cond, readRelation)

  // 4. 是否需要基数检查
  val performCardinalityCheck = isCardinalityCheckNeeded(matchedActions)

  // 5. 为 Target 表添加标记列
  val rowFromTarget = Alias(TrueLiteral, ROW_FROM_TARGET)()
  val targetTableProjExprs = if (performCardinalityCheck) {
    val rowId = Alias(MonotonicallyIncreasingID(), ROW_ID)()
    readAttrs ++ Seq(rowFromTarget, rowId)
  } else {
    readAttrs :+ rowFromTarget
  }
  // ⚠️ 注意：这里 Filter(targetCond, readRelation) 实现了条件下推
  val targetTableProj = Project(targetTableProjExprs, Filter(targetCond, readRelation))

  // 6. 为 Source 表添加标记列
  val sourceTableProjExprs = source.output :+ Alias(TrueLiteral, ROW_FROM_SOURCE)()
  val sourceTableProj = Project(sourceTableProjExprs, source)

  // 7. 构建 JOIN
  //    - 如果没有 NOT MATCHED 操作，使用 INNER JOIN
  //    - 否则使用 RIGHT OUTER JOIN
  val joinType = if (notMatchedActions.isEmpty) Inner else RightOuter
  val joinHint = JoinHint(leftHint = Some(HintInfo(Some(NO_BROADCAST_HASH))), rightHint = None)
  val joinPlan = Join(NoStatsUnaryNode(targetTableProj), sourceTableProj, joinType, Some(joinCond), joinHint)

  // 8. 构建 Matched 和 Not Matched 输出
  val metadataReadAttrs = readAttrs.filterNot(relation.outputSet.contains)

  val matchedConditions = matchedActions.map(actionCondition)
  val matchedOutputs = matchedActions.map { action =>
    // MoR 模式：UPDATE 生成 DELETE + INSERT
    matchedDeltaActionOutput(action, rowAttrs, rowIdAttrs, metadataReadAttrs)
  }

  val notMatchedConditions = notMatchedActions.map(actionCondition)
  val notMatchedOutputs = notMatchedActions.map { action =>
    notMatchedDeltaActionOutput(action, metadataReadAttrs)
  }

  // 9. 构建 MergeRows 操作
  val operationTypeAttr = AttributeReference(OPERATION_COLUMN, IntegerType, nullable = false)()
  val rowFromSourceAttr = resolveAttrRef(ROW_FROM_SOURCE_REF, joinPlan)
  val rowFromTargetAttr = resolveAttrRef(ROW_FROM_TARGET_REF, joinPlan)

  val mergeRowsOutput = buildMergeRowsOutput(matchedOutputs, notMatchedOutputs, operationTypeAttr +: readAttrs)

  val mergeRows = MergeRows(
    isSourceRowPresent = IsNotNull(rowFromSourceAttr),
    isTargetRowPresent = if (notMatchedActions.isEmpty) TrueLiteral else IsNotNull(rowFromTargetAttr),
    matchedConditions = matchedConditions,
    matchedOutputs = matchedOutputs,
    notMatchedConditions = notMatchedConditions,
    notMatchedOutputs = notMatchedOutputs,
    targetOutput = Nil,  // MoR 不需要 emit 未匹配的 target 行
    performCardinalityCheck = performCardinalityCheck,
    emitNotMatchedTargetRows = false,  // MoR 模式不需要保留未匹配的 target 行
    output = mergeRowsOutput,
    joinPlan)

  // 10. 构建 Delta Projections
  //     分离 DELETE 和 INSERT 操作
  val writeRelation = relation.copy(table = operationTable)
  val projections = buildDeltaProjections(mergeRows, rowAttrs, rowIdAttrs, metadataAttrs)

  WriteIcebergDelta(writeRelation, mergeRows, relation, projections)
}
```

### 5.3 matchedDeltaActionOutput：UPDATE 生成 DELETE + INSERT

```scala
private def matchedDeltaActionOutput(
    action: MergeAction,
    rowAttrs: Seq[Attribute],
    rowIdAttrs: Seq[Attribute],
    metadataAttrs: Seq[Attribute]): Seq[Seq[Expression]] = {

  action match {
    case u: UpdateAction =>
      // UPDATE 操作生成两行：DELETE + INSERT
      val delete = deltaDeleteOutput(rowAttrs, rowIdAttrs, metadataAttrs)
      val insert = deltaInsertOutput(u.assignments.map(_.value), metadataAttrs)
      Seq(delete, insert)

    case _: DeleteAction =>
      // DELETE 操作只生成一行：DELETE
      val delete = deltaDeleteOutput(rowAttrs, rowIdAttrs, metadataAttrs)
      Seq(delete)

    case other =>
      throw new AnalysisException(s"Unexpected WHEN MATCHED action: $other")
  }
}

// DELETE 行的输出结构
private def deltaDeleteOutput(
    rowAttrs: Seq[Attribute],
    rowIdAttrs: Seq[Attribute],
    metadataAttrs: Seq[Attribute]): Seq[Expression] = {

  Seq(
    Literal(DELETE_OPERATION),  // operation_type = DELETE
    // ... 其他列为 NULL 或保留原值（用于生成 Position Delete）
  )
}

// INSERT 行的输出结构
private def deltaInsertOutput(
    values: Seq[Expression],
    metadataAttrs: Seq[Attribute]): Seq[Expression] = {

  Seq(
    Literal(INSERT_OPERATION),  // operation_type = INSERT
    // ... 新值
  )
}
```

### 5.4 Position Delete 生成

```
在 MoR 模式中，UPDATE 操作的处理：

原始数据（data/f1.parquet）：
  ┌────┬────────┬─────┐
  │ id │ name   │ age │
  ├────┼────────┼─────┤
  │ 1  │ Alice  │ 25  │  ← 行号 0
  │ 2  │ Bob    │ 30  │  ← 行号 1
  │ 3  │ Carol  │ 28  │  ← 行号 2
  └────┴────────┴─────┘

UPDATE id=2 SET age=35:
  ↓
生成 DELETE 行：
  { operation: DELETE, file_path: "data/f1.parquet", pos: 1 }
  ↓
写入 delete/d1.parquet (Position Delete):
  ┌─────────────────┬─────┐
  │ file_path       │ pos │
  ├─────────────────┼─────┤
  │ data/f1.parquet │ 1   │
  └─────────────────┴─────┘

生成 INSERT 行：
  { operation: INSERT, id: 2, name: "Bob", age: 35 }
  ↓
写入 data/f2.parquet:
  ┌────┬────────┬─────┐
  │ id │ name   │ age │
  ├────┼────────┼─────┤
  │ 2  │ Bob    │ 35  │
  └────┴────────┴─────┘

读取时合并：
  1. 读取 data/f1.parquet → 3 rows
  2. 读取 delete/d1.parquet → pos=1 被标记删除
  3. 过滤：跳过 pos=1 的行
  4. 读取 data/f2.parquet → 1 row
  5. 最终结果：
     ┌────┬────────┬─────┐
     │ id │ name   │ age │
     ├────┼────────┼─────┤
     │ 1  │ Alice  │ 25  │
     │ 3  │ Carol  │ 28  │
     │ 2  │ Bob    │ 35  │ ← 更新后的行
     └────┴────────┴─────┘
```

### 5.5 MoR 模式特点

```
优点：
  ✅ 写入性能高
     - 只写入删除文件和新增文件
     - 不重写现有数据文件
     - 写放大小

  ✅ 并发写入能力强
     - 不同分区可以并行写入
     - 减少锁竞争

  ✅ 适合高频更新
     - CDC (Change Data Capture)
     - 实时 OLTP 场景

缺点：
  ⚠️  读取性能稍低
     - 需要合并 Data Files 和 Delete Files
     - 额外的 I/O 和计算开销

  ⚠️  Delete Files 累积
     - 需要定期 Compaction
     - 否则影响读取性能

  ⚠️  Compaction 复杂
     - 需要处理 Position Delete 和 Equality Delete
     - 需要重写数据文件

适用场景：
  🔄 实时 CDC
  🔄 高频 UPSERT
  🔄 流式更新
  🔄 OLTP 事务场景
```

---

## 6. CoW vs MoR 对比

### 6.1 性能对比

```
场景：1TB 表，UPDATE 1% 的数据（10GB）

┌─────────────────────────────────────────────────────────────┐
│              Copy-on-Write 模式                               │
├─────────────────────────────────────────────────────────────┤
│  写入操作：                                                    │
│    - 读取受影响的文件：10GB                                    │
│    - 在内存中 UPDATE                                          │
│    - 重写文件：10GB                                           │
│  ↓                                                           │
│  总 I/O: 20GB (10GB read + 10GB write)                      │
│  写入耗时: ~5 分钟（假设 50MB/s 写入速度）                      │
│                                                              │
│  读取操作：                                                    │
│    - 直接读取数据文件                                          │
│    - 无需合并 Delete Files                                    │
│  ↓                                                           │
│  读取耗时: ~1 分钟                                             │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│              Merge-on-Read 模式                               │
├─────────────────────────────────────────────────────────────┤
│  写入操作：                                                    │
│    - 生成 Delete File: ~10MB (Position Delete)               │
│    - 生成新 Data File: ~10GB（包含更新后的数据）               │
│  ↓                                                           │
│  总 I/O: ~10GB (仅写入)                                       │
│  写入耗时: ~3 分钟                                             │
│                                                              │
│  读取操作：                                                    │
│    - 读取数据文件：10GB                                        │
│    - 读取 Delete Files：10MB                                 │
│    - 合并逻辑：过滤删除的行                                     │
│  ↓                                                           │
│  读取耗时: ~1.5 分钟                                           │
└─────────────────────────────────────────────────────────────┘

结论：
  - 写入：MoR 快 40%（5 分钟 vs 3 分钟）
  - 读取：CoW 快 30%（1 分钟 vs 1.5 分钟）
```

### 6.2 适用场景对比

| 维度 | Copy-on-Write | Merge-on-Read |
|-----|--------------|--------------|
| **写入频率** | 低频（小时/天级） | 高频（秒/分级） |
| **数据规模** | 大批量（GB级） | 小批量（MB级） |
| **并发写入** | 较弱 | 较强 |
| **读取性能** | 高（直接读取） | 中（需合并） |
| **写入性能** | 低（写放大） | 高（增量） |
| **Compaction** | 简单 | 复杂 |
| **存储占用** | 较低 | 稍高（Delete Files） |
| **典型场景** | OLAP、ETL | OLTP、CDC |

### 6.3 配置选择

```properties
# Copy-on-Write 配置（默认）
write.merge.mode=copy-on-write
write.update.mode=copy-on-write
write.delete.mode=copy-on-write

# Merge-on-Read 配置
write.merge.mode=merge-on-read
write.update.mode=merge-on-read
write.delete.mode=merge-on-read

# 混合配置（可以针对不同操作配置不同模式）
write.merge.mode=merge-on-read  # MERGE INTO 使用 MoR
write.update.mode=merge-on-read # UPDATE 使用 MoR
write.delete.mode=copy-on-write # DELETE 使用 CoW
```

---

## 7. MergeRows 执行器

### 7.1 MergeRowsExec 源码位置

**文件**: `spark/v3.4/spark-extensions/src/main/scala/org/apache/spark/sql/execution/datasources/v2/MergeRowsExec.scala`

### 7.2 执行逻辑

```scala
case class MergeRows(
    isSourceRowPresent: Expression,      // 是否有 source 行
    isTargetRowPresent: Expression,      // 是否有 target 行
    matchedConditions: Seq[Expression],  // MATCHED 条件
    matchedOutputs: Seq[Seq[Expression]], // MATCHED 输出
    notMatchedConditions: Seq[Expression], // NOT MATCHED 条件
    notMatchedOutputs: Seq[Expression],  // NOT MATCHED 输出
    targetOutput: Seq[Attribute],        // Target 行输出（CoW 模式）
    performCardinalityCheck: Boolean,    // 是否执行基数检查
    emitNotMatchedTargetRows: Boolean,   // 是否输出未匹配的 target 行
    output: Seq[Attribute],
    child: LogicalPlan) extends UnaryNode

// 物理执行逻辑（简化）
override protected def doExecute(): RDD[InternalRow] = {
  child.execute().mapPartitions { rows =>
    rows.flatMap { row =>
      val sourcePresent = isSourceRowPresent.eval(row).asInstanceOf[Boolean]
      val targetPresent = isTargetRowPresent.eval(row).asInstanceOf[Boolean]

      if (sourcePresent && targetPresent) {
        // MATCHED 情况
        handleMatchedRow(row)
      } else if (sourcePresent && !targetPresent) {
        // NOT MATCHED 情况
        handleNotMatchedRow(row)
      } else if (!sourcePresent && targetPresent) {
        // Target-only（仅 CoW 模式）
        if (emitNotMatchedTargetRows) {
          Some(row)
        } else {
          None
        }
      } else {
        None
      }
    }
  }
}

private def handleMatchedRow(row: InternalRow): Iterator[InternalRow] = {
  // 遍历所有 MATCHED 操作
  var i = 0
  while (i < matchedConditions.length) {
    if (matchedConditions(i).eval(row).asInstanceOf[Boolean]) {
      // 条件匹配，应用对应的输出
      val outputs = matchedOutputs(i)
      if (outputs.isEmpty) {
        // DELETE 操作 → 不输出行
        return Iterator.empty
      } else {
        // UPDATE 操作 → 输出更新后的行
        // MoR 模式：可能输出多行（DELETE + INSERT）
        return outputs.map(output => projectRow(output, row))
      }
    }
    i += 1
  }
  // 如果没有匹配的 MATCHED 条件，保留原行
  Iterator.single(row)
}

private def handleNotMatchedRow(row: InternalRow): Iterator[InternalRow] = {
  var i = 0
  while (i < notMatchedConditions.length) {
    if (notMatchedConditions(i).eval(row).asInstanceOf[Boolean]) {
      // INSERT 操作
      return Iterator.single(projectRow(notMatchedOutputs(i), row))
    }
    i += 1
  }
  Iterator.empty
}
```

### 7.3 基数检查（Cardinality Check）

```
问题：一个 target 行匹配多个 source 行

示例：
  Target:
    ┌────┬────────┐
    │ id │ name   │
    ├────┼────────┤
    │ 1  │ Alice  │
    └────┴────────┘

  Source:
    ┌────┬────────┐
    │ id │ name   │
    ├────┼────────┤
    │ 1  │ Alice2 │
    │ 1  │ Alice3 │ ← 同一个 id 有两行
    └────┴────────┘

  MERGE INTO target USING source ON target.id = source.id
  WHEN MATCHED THEN UPDATE SET *

  JOIN 结果：
    ┌───────────┬──────────┬───────────┬──────────┐
    │ target.id │ t.name   │ source.id │ s.name   │
    ├───────────┼──────────┼───────────┼──────────┤
    │ 1         │ Alice    │ 1         │ Alice2   │ ← Matched
    │ 1         │ Alice    │ 1         │ Alice3   │ ← Matched (重复！)
    └───────────┴──────────┴───────────┴──────────┘

  问题：
    id=1 的 target 行会被 UPDATE 两次
    ↓
    数据不一致！最终 name 是 Alice2 还是 Alice3？

  解决方案：Cardinality Check
    ↓
    为每个 target 行分配唯一 ID（MonotonicallyIncreasingID）
    ↓
    检测到同一个 ID 出现多次 → 抛出异常
    ↓
    MERGE INTO 失败，要求用户修复 source 数据

基数检查实现（简化）：
  val rowIdToCount = mutable.Map[Long, Int]()
  rows.foreach { row =>
    val rowId = row.getLong(rowIdIndex)
    if (rowIdToCount.contains(rowId)) {
      throw new SparkException(
        s"MERGE INTO target matched a single row from the target table " +
        s"with multiple rows of the source table. This could result in " +
        s"the target row being operated on more than once with undefined results."
      )
    }
    rowIdToCount(rowId) = 1
  }
```

---

## 8. 完整示例

### 8.1 CoW 模式示例

```scala
// 配置 CoW 模式
spark.sql("""
  ALTER TABLE iceberg_catalog.db.users
  SET TBLPROPERTIES (
    'write.merge.mode' = 'copy-on-write'
  )
""")

// 执行 MERGE INTO
spark.sql("""
  MERGE INTO iceberg_catalog.db.users AS target
  USING updates AS source
  ON target.dt = source.dt AND target.id = source.id
  WHEN MATCHED THEN
    UPDATE SET target.name = source.name, target.age = source.age
  WHEN NOT MATCHED THEN
    INSERT (id, name, age, dt) VALUES (source.id, source.name, source.age, source.dt)
""")

// 执行流程：
// 1. RewriteMergeIntoTable 识别为 CoW 模式
// 2. buildReplaceDataPlan 构建 FULL OUTER JOIN
// 3. MergeRows 执行合并逻辑
//    - MATCHED: 输出更新后的行
//    - NOT MATCHED: 输出新插入的行
//    - Target-only: 输出原行
// 4. ReplaceIcebergData 重写数据文件
// 5. OverwriteFiles API 提交

// 文件变化：
// Before:
//   data/dt=2024-10-26/f1.parquet (100MB)
// After:
//   data/dt=2024-10-26/f2.parquet (105MB)
//   (f1.parquet 在元数据中标记删除)
```

### 8.2 MoR 模式示例

```scala
// 配置 MoR 模式
spark.sql("""
  ALTER TABLE iceberg_catalog.db.users
  SET TBLPROPERTIES (
    'write.merge.mode' = 'merge-on-read'
  )
""")

// 执行 MERGE INTO
spark.sql("""
  MERGE INTO iceberg_catalog.db.users AS target
  USING updates AS source
  ON target.dt = source.dt AND target.id = source.id
  WHEN MATCHED THEN
    UPDATE SET target.name = source.name, target.age = source.age
  WHEN NOT MATCHED THEN
    INSERT (id, name, age, dt) VALUES (source.id, source.name, source.age, source.dt)
""")

// 执行流程：
// 1. RewriteMergeIntoTable 识别为 MoR 模式
// 2. buildWriteDeltaPlan 构建 RIGHT OUTER JOIN
// 3. MergeRows 执行合并逻辑
//    - MATCHED: 输出 DELETE 行 + INSERT 行
//    - NOT MATCHED: 输出 INSERT 行
// 4. WriteDeltaProjections 分离 DELETE 和 INSERT
// 5. WriteIcebergDelta 写入 Delete Files 和 Data Files
// 6. RowDelta API 提交

// 文件变化：
// Before:
//   data/dt=2024-10-26/f1.parquet (100MB)
// After:
//   data/dt=2024-10-26/f1.parquet (100MB, 保持不变)
//   data/dt=2024-10-26/f2.parquet (5MB, 新数据)
//   delete/dt=2024-10-26/d1.parquet (10KB, Position Delete)
```

---

## 9. Compaction：Delete Files 清理

### 9.1 为什么需要 Compaction

```
MoR 模式的问题：
  随着 UPDATE/DELETE 操作累积，Delete Files 越来越多

  初始状态：
    data/f1.parquet (1000 rows)

  After 100 UPDATEs:
    data/f1.parquet (1000 rows)
    data/f2.parquet (10 rows)
    data/f3.parquet (10 rows)
    ...
    data/f101.parquet (10 rows)
    delete/d1.parquet (10 deletes)
    delete/d2.parquet (10 deletes)
    ...
    delete/d100.parquet (10 deletes)

  读取时：
    1. 读取 f1.parquet
    2. 读取 100 个 delete files
    3. 合并逻辑：过滤 1000 个 deletes
    ↓
    性能严重下降！
```

### 9.2 Compaction 策略

```scala
// 使用 Spark SQL 触发 Compaction
spark.sql("""
  CALL iceberg_catalog.system.rewrite_data_files(
    table => 'db.users',
    where => "dt = '2024-10-26'"
  )
""")

// Compaction 流程：
// 1. 读取分区内所有文件：
//    data/f1.parquet + delete/d1~d100.parquet
// 2. 应用 Delete Files，生成干净的数据
// 3. 重写为新文件：
//    data/f102.parquet (900 rows)
// 4. 删除旧文件：
//    f1.parquet, d1~d100.parquet

// 效果：
//   - 减少文件数量：101 → 1
//   - 消除 Delete Files
//   - 提升读取性能
```

### 9.3 自动 Compaction

```properties
# 配置自动 Compaction 触发条件
write.metadata.delete-after-commit.enabled=true
write.metadata.previous-versions-max=5

# 当 Delete Files 数量超过阈值时触发 Compaction
write.delete-files-threshold=10

# 定期调度 Compaction（推荐）
# 使用 Spark SQL 定时任务或 Airflow
*/

0 2 * * * spark-sql -e "
  CALL iceberg_catalog.system.rewrite_data_files(
    table => 'db.users',
    options => map('target-file-size-bytes', '536870912')
  )
"
```

---

## 10. 总结

### 10.1 关键要点

1. **MERGE INTO 是 UPSERT 的标准实现**
   - 支持 MATCHED UPDATE/DELETE
   - 支持 NOT MATCHED INSERT

2. **两种执行模式**：
   - CoW: 重写数据文件（读快写慢）
   - MoR: 生成 Delete Files（写快读慢）

3. **模式选择**：
   - 通过 `write.merge.mode` 配置
   - 根据场景选择：OLAP → CoW，OLTP → MoR

4. **核心优化**：
   - 分区裁剪：减少扫描范围
   - 基数检查：防止数据不一致
   - Compaction：清理 Delete Files

### 10.2 最佳实践

| 场景 | 模式 | 配置 |
|-----|------|------|
| 批量 ETL | CoW | `write.merge.mode=copy-on-write` |
| 实时 CDC | MoR | `write.merge.mode=merge-on-read` |
| 定时更新 | CoW | `write.merge.mode=copy-on-write` |
| 高频 UPSERT | MoR | `write.merge.mode=merge-on-read` + 定期 Compaction |

### 10.3 性能调优

```properties
# 1. 分区策略
# 确保 MERGE INTO 的 ON 条件包含分区列
# ✅ ON target.dt = source.dt AND target.id = source.id
# ❌ ON target.id = source.id (全表扫描)

# 2. 文件大小
write.target-file-size-bytes=536870912  # 512MB

# 3. Compaction 频率
# MoR 模式：每天执行一次 Compaction
# CoW 模式：不需要 Compaction

# 4. 并行度
spark.sql.shuffle.partitions=200  # 根据数据量调整

# 5. Join 优化
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true
```

---

## 11. 参考资源

- **源码位置**：
  - `spark-extensions/.../MergeIntoIcebergTable.scala`
  - `spark-extensions/.../RewriteMergeIntoTable.scala`
  - `spark-extensions/.../MergeRowsExec.scala`
  - `spark-extensions/.../ReplaceIcebergData.scala`
  - `spark-extensions/.../WriteIcebergDelta.scala`

- **相关文档**：
  - `iceberg-write-flow.md` - Iceberg 写入流程
  - `iceberg-read-flow.md` - Iceberg 读取流程

- **示例代码**：
  - `SparkIcebergCowSuite.scala` - CoW 模式示例
  - `SparkIcebergMorSuite.scala` - MoR 模式示例
  - `SparkWriteWithPartitionPrimaryKeySuite.scala` - MERGE INTO 示例

---

**文档版本**: v1.0
**最后更新**: 2025-10-26
**作者**: Claude Code (基于源码分析)
