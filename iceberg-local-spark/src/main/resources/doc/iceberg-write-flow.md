# Apache Iceberg 写入流程详解

> 基于 Apache Iceberg 1.7.0-SNAPSHOT 源码分析
>
> 文档目标：深入理解 Iceberg 的写入机制，包括 Append、Overwrite、RowDelta 等操作

---

## 1. 架构概览

### 1.1 核心概念

Iceberg 使用 **Snapshot（快照）** 作为表状态的不可变版本，每次写入操作都会生成新的快照。

```
Table
  └── Snapshot (当前快照)
       ├── Manifest List (清单列表文件)
       │    ├── Manifest File 1 (数据清单)
       │    │    ├── DataFile 1
       │    │    ├── DataFile 2
       │    │    └── ...
       │    ├── Manifest File 2 (删除清单)
       │    │    ├── DeleteFile 1
       │    │    ├── DeleteFile 2
       │    │    └── ...
       │    └── ...
       └── Parent Snapshot (父快照 ID)
```

### 1.2 写入操作分类

| 操作类型 | API 接口 | 用途 | 生成的快照类型 |
|---------|---------|------|--------------|
| **Append** | `AppendFiles` | 追加新数据 | append |
| **Overwrite** | `OverwriteFiles` | 覆盖数据（CoW 模式的 UPDATE/DELETE） | overwrite |
| **RowDelta** | `RowDelta` | 行级变更（MoR 模式的 UPDATE/DELETE） | overwrite |
| **Delete** | `DeleteFiles` | 删除文件（元数据操作） | delete |

---

## 2. AppendFiles：追加写入

### 2.1 API 定义

**文件路径**: `api/src/main/java/org/apache/iceberg/AppendFiles.java`

```java
public interface AppendFiles extends SnapshotUpdate<AppendFiles> {
  /**
   * Append a {@link DataFile} to the table.
   */
  AppendFiles appendFile(DataFile file);

  /**
   * Append a {@link ManifestFile} to the table.
   *
   * 支持直接追加 Manifest 文件（format v2 + snapshot ID inheritance）
   */
  AppendFiles appendManifest(ManifestFile file);
}
```

### 2.2 执行流程

```
┌──────────────────────────────────────────────────────────────────┐
│                    AppendFiles 执行流程                            │
└──────────────────────────────────────────────────────────────────┘

1. 创建 AppendFiles 实例
   ↓
   table.newAppend()
   ↓
2. 添加数据文件
   ↓
   append.appendFile(dataFile)
   append.appendFile(dataFile2)
   ...
   ↓
3. 提交（commit）
   ↓
   append.commit()
   ↓
   ┌─────────────────────────────────────┐
   │  Iceberg 内部处理                     │
   ├─────────────────────────────────────┤
   │ 3.1 读取当前 Snapshot                 │
   │ 3.2 创建或更新 Manifest File          │
   │     - 将新 DataFile 写入 Manifest    │
   │     - 如果使用 snapshot ID           │
   │       inheritance，直接复用 Manifest │
   │ 3.3 创建新 Manifest List              │
   │     - 包含旧 Manifest + 新 Manifest  │
   │ 3.4 创建新 Snapshot                   │
   │     - parent_snapshot_id: 旧快照 ID  │
   │     - operation: "append"            │
   │     - manifest_list: 新清单列表路径   │
   │ 3.5 更新 Table Metadata               │
   │     - current_snapshot_id: 新快照 ID │
   │ 3.6 原子性提交元数据文件               │
   │     - 使用 CAS (Compare-And-Swap)    │
   │     - 如果冲突，重试整个流程           │
   └─────────────────────────────────────┘
   ↓
4. 提交成功
   新快照成为表的当前版本
```

### 2.3 关键特性

#### 2.3.1 冲突解决（Optimistic Concurrency Control）

```
Timeline:
  T1: Transaction A 读取 Snapshot 100
  T2: Transaction B 读取 Snapshot 100
  T3: Transaction A 提交 → Snapshot 101 成功
  T4: Transaction B 提交 → 检测到 Snapshot 已变为 101
      ↓
      自动重试：
      - 在 Snapshot 101 基础上应用 Transaction B 的变更
      - 提交 Snapshot 102
```

这就是 Iceberg 的 **乐观并发控制** 机制，通过自动重试保证 ACID 事务语义。

#### 2.3.2 Manifest 复用（Snapshot ID Inheritance）

**format-version=2** 支持 Snapshot ID Inheritance：

```
不使用继承（v1 或禁用）:
  Snapshot 100 → Manifest A (包含 File1, File2)
  Snapshot 101 → Manifest B (重写，包含 File1, File2, File3)

使用继承（v2 + 启用）:
  Snapshot 100 → Manifest A (包含 File1, File2)
  Snapshot 101 → Manifest A (复用) + Manifest C (仅包含 File3)

优势：
  ✅ 减少 Manifest 重写
  ✅ 提高写入性能
  ✅ 降低存储开销
```

### 2.4 Spark SQL 示例

```scala
// Spark 3.x + Iceberg
val df = Seq(
  (1, "Alice", "2024-10-26"),
  (2, "Bob", "2024-10-26")
).toDF("id", "name", "dt")

// 追加写入
df.writeTo("iceberg_catalog.db.table")
  .append()

// 等价于：
// table.newAppend()
//   .appendFile(dataFile1)
//   .appendFile(dataFile2)
//   .commit()
```

---

## 3. OverwriteFiles：覆盖写入

### 3.1 API 定义

**文件路径**: `api/src/main/java/org/apache/iceberg/OverwriteFiles.java`

```java
public interface OverwriteFiles extends SnapshotUpdate<OverwriteFiles> {
  /**
   * Delete files that match an {@link Expression} on data rows.
   *
   * 使用表达式删除文件（必须是分区级别的精确匹配）
   */
  OverwriteFiles overwriteByRowFilter(Expression expr);

  /**
   * Add a {@link DataFile} to the table.
   */
  OverwriteFiles addFile(DataFile file);

  /**
   * Delete a {@link DataFile} from the table.
   */
  OverwriteFiles deleteFile(DataFile file);

  /**
   * Signal that each file added must match the overwrite expression.
   *
   * 幂等性验证：确保新增文件匹配覆盖条件
   */
  OverwriteFiles validateAddedFilesMatchOverwriteFilter();
}
```

### 3.2 执行流程

```
┌──────────────────────────────────────────────────────────────────┐
│                  OverwriteFiles 执行流程                           │
└──────────────────────────────────────────────────────────────────┘

1. 创建 OverwriteFiles 实例
   ↓
   table.newOverwrite()
   ↓
2. 指定覆盖范围（两种方式）
   ↓
   方式1: 使用表达式（推荐）
   overwrite.overwriteByRowFilter(Expressions.equal("dt", "2024-10-26"))

   方式2: 手动指定删除和添加文件
   overwrite.deleteFile(oldDataFile1)
   overwrite.deleteFile(oldDataFile2)
   ↓
3. 添加新数据文件
   ↓
   overwrite.addFile(newDataFile1)
   overwrite.addFile(newDataFile2)
   ↓
4. 提交
   ↓
   overwrite.commit()
   ↓
   ┌─────────────────────────────────────┐
   │  Iceberg 内部处理                     │
   ├─────────────────────────────────────┤
   │ 4.1 如果使用 overwriteByRowFilter:   │
   │     - 使用 Inclusive Projection      │
   │       找到可能包含匹配行的文件         │
   │     - 使用 Strict Projection         │
   │       验证文件所有行都匹配             │
   │     - 删除验证通过的文件               │
   │                                      │
   │ 4.2 更新 Manifest File:              │
   │     - 标记删除的文件为 DELETED        │
   │     - 添加新文件为 ADDED              │
   │                                      │
   │ 4.3 创建新 Snapshot:                 │
   │     - operation: "overwrite"         │
   │     - summary:                       │
   │       - deleted-data-files: 2        │
   │       - added-data-files: 2          │
   │                                      │
   │ 4.4 冲突检测（如果启用）:             │
   │     - validateNoConflictingData()    │
   │     - validateNoConflictingDeletes() │
   └─────────────────────────────────────┘
   ↓
5. 提交成功
```

### 3.3 Overwrite 模式

#### 3.3.1 静态分区覆盖（Static Partition Overwrite）

```scala
// 覆盖整个分区 dt=2024-10-26
df.writeTo("iceberg_catalog.db.table")
  .overwritePartitions()

// 等价于：
// overwrite.overwriteByRowFilter(equal("dt", "2024-10-26"))
//   .addFile(newFile1)
//   .addFile(newFile2)
//   .commit()
```

#### 3.3.2 动态分区覆盖（Dynamic Partition Overwrite）

```scala
// 仅覆盖 df 中实际包含的分区
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

df.writeTo("iceberg_catalog.db.table")
  .overwritePartitions()

// 如果 df 只有 dt=2024-10-26 和 dt=2024-10-27
// 则只覆盖这两个分区，其他分区保持不变
```

### 3.4 幂等性保证

```java
overwrite
  .overwriteByRowFilter(equal("dt", "2024-10-26"))
  .validateAddedFilesMatchOverwriteFilter()  // 启用幂等性验证
  .addFile(newFile)
  .commit();

// 验证逻辑：
// 1. 检查 newFile 的分区值是否匹配 dt=2024-10-26
// 2. 如果不匹配，抛出 ValidationException
// 3. 确保重复执行操作结果一致
```

---

## 4. RowDelta：行级变更

### 4.1 API 定义

**文件路径**: `api/src/main/java/org/apache/iceberg/RowDelta.java`

```java
public interface RowDelta extends SnapshotUpdate<RowDelta> {
  /**
   * Add a {@link DataFile} to the table (插入新行).
   */
  RowDelta addRows(DataFile inserts);

  /**
   * Add a {@link DeleteFile} to the table (删除行).
   *
   * 支持两种 Delete File:
   * - Position Delete: 记录文件名 + 行号
   * - Equality Delete: 记录主键值
   */
  RowDelta addDeletes(DeleteFile deletes);

  /**
   * Sets a conflict detection filter.
   *
   * 用于检测并发冲突
   */
  RowDelta conflictDetectionFilter(Expression conflictDetectionFilter);

  /**
   * Enables validation that data files added concurrently do not conflict.
   *
   * 保证 Serializable 隔离级别
   */
  RowDelta validateNoConflictingDataFiles();

  /**
   * Enables validation that delete files added concurrently do not conflict.
   *
   * UPDATE/MERGE 操作必须调用
   */
  RowDelta validateNoConflictingDeleteFiles();
}
```

### 4.2 Delete File 类型

#### 4.2.1 Position Delete

```
记录需要删除的行的位置

文件格式：
┌─────────────────┬──────────────┐
│ file_path       │ pos          │
├─────────────────┼──────────────┤
│ data/f1.parquet │ 10           │  ← 删除 f1.parquet 的第 10 行
│ data/f1.parquet │ 25           │  ← 删除 f1.parquet 的第 25 行
│ data/f2.parquet │ 3            │  ← 删除 f2.parquet 的第 3 行
└─────────────────┴──────────────┘

优点：
  ✅ 精确定位
  ✅ 适合按文件组织的数据

缺点：
  ⚠️  依赖数据文件
  ⚠️  数据文件 compaction 后失效
```

#### 4.2.2 Equality Delete

```
记录需要删除的行的主键值

文件格式（假设主键是 id）：
┌──────┐
│ id   │
├──────┤
│ 123  │  ← 删除所有 id=123 的行
│ 456  │  ← 删除所有 id=456 的行
│ 789  │  ← 删除所有 id=789 的行
└──────┘

优点：
  ✅ 不依赖数据文件
  ✅ 支持表级主键
  ✅ 数据文件 compaction 后仍有效

缺点：
  ⚠️  需要主键列
  ⚠️  读取时需要按主键匹配（性能开销）
```

### 4.3 执行流程

```
┌──────────────────────────────────────────────────────────────────┐
│                    RowDelta 执行流程                               │
└──────────────────────────────────────────────────────────────────┘

1. 创建 RowDelta 实例
   ↓
   table.newRowDelta()
   ↓
2. 添加行级变更
   ↓
   rowDelta.addRows(newDataFile)        // 插入新行
   rowDelta.addDeletes(deleteFile)       // 删除行
   ↓
3. 设置冲突检测（可选但推荐）
   ↓
   rowDelta.conflictDetectionFilter(filter)
   rowDelta.validateNoConflictingDataFiles()
   rowDelta.validateNoConflictingDeleteFiles()
   ↓
4. 提交
   ↓
   rowDelta.commit()
   ↓
   ┌─────────────────────────────────────┐
   │  Iceberg 内部处理                     │
   ├─────────────────────────────────────┤
   │ 4.1 验证 Delete File:                │
   │     - Position Delete: 验证引用的    │
   │       数据文件存在                    │
   │     - Equality Delete: 验证主键列    │
   │       存在                           │
   │                                      │
   │ 4.2 更新 Manifest:                   │
   │     - Data Manifest: 添加新数据文件  │
   │     - Delete Manifest: 添加删除文件  │
   │                                      │
   │ 4.3 创建新 Snapshot:                 │
   │     - operation: "overwrite"         │
   │     - summary:                       │
   │       - added-data-files: 1          │
   │       - added-delete-files: 1        │
   │       - deleted-records: 100         │
   │                                      │
   │ 4.4 冲突检测:                         │
   │     - 检查并发的 Data File 添加       │
   │     - 检查并发的 Delete File 添加     │
   │     - 如果冲突，重试或失败             │
   └─────────────────────────────────────┘
   ↓
5. 提交成功
```

### 4.4 MoR 模式 UPDATE 示例

```scala
// Spark SQL: UPDATE users SET age = 30 WHERE id = 123

步骤分解：
1. 读取表，找到 id=123 的行所在的数据文件
   ↓
   数据文件: data/users/f1.parquet (包含 id=123 的行，位于第 10 行)

2. 生成 Delete File (Position Delete)
   ↓
   delete-file: delete/d1.parquet
   内容: file_path = data/users/f1.parquet, pos = 10

3. 生成新 Data File (包含更新后的数据)
   ↓
   new-file: data/users/f2.parquet
   内容: id=123, age=30

4. 提交 RowDelta
   ↓
   rowDelta.addDeletes(d1.parquet)
   rowDelta.addRows(f2.parquet)
   rowDelta.commit()

结果：
  - 旧数据文件 f1.parquet 保持不变（不重写）
  - 新增 delete file d1.parquet 标记删除旧行
  - 新增 data file f2.parquet 包含新行
  - 读取时：Iceberg 自动合并 data file 和 delete file
```

---

## 5. 写入流程对比

### 5.1 Append vs Overwrite vs RowDelta

| 维度 | AppendFiles | OverwriteFiles | RowDelta |
|-----|-------------|----------------|----------|
| **操作类型** | INSERT | UPDATE/DELETE (CoW) | UPDATE/DELETE (MoR) |
| **是否删除数据文件** | ❌ 否 | ✅ 是 | ❌ 否 |
| **是否生成 Delete File** | ❌ 否 | ❌ 否 | ✅ 是 |
| **写放大** | 低 | 高（重写整个文件） | 低（仅写删除文件） |
| **读性能** | 高 | 高 | 中（需合并删除） |
| **适用场景** | 批量导入 | OLAP 批处理 | OLTP 实时更新 |

### 5.2 Copy-on-Write vs Merge-on-Read

```
┌─────────────────────────────────────────────────────────────┐
│              Copy-on-Write (使用 OverwriteFiles)              │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  原始数据: f1.parquet (100MB, 包含 id=1~1000)                 │
│  UPDATE: id=500 的记录                                        │
│  ↓                                                            │
│  1. 读取 f1.parquet                                           │
│  2. 在内存中修改 id=500 的记录                                 │
│  3. 重写整个文件 → f2.parquet (100MB)                         │
│  4. 删除 f1.parquet（元数据中标记删除）                         │
│  ↓                                                            │
│  结果: 只保留 f2.parquet                                       │
│                                                               │
│  优点: ✅ 读取性能高（无需合并）                                │
│  缺点: ⚠️  写放大严重（100MB 写入更新 1 条记录）                │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│              Merge-on-Read (使用 RowDelta)                    │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  原始数据: f1.parquet (100MB, 包含 id=1~1000)                 │
│  UPDATE: id=500 的记录                                        │
│  ↓                                                            │
│  1. 查找 id=500 在 f1.parquet 的位置（第 500 行）              │
│  2. 生成 Delete File: delete/d1.parquet (1KB)                │
│     内容: file_path=f1.parquet, pos=500                       │
│  3. 生成新 Data File: f2.parquet (1KB)                        │
│     内容: id=500 的新数据                                      │
│  ↓                                                            │
│  结果: 保留 f1.parquet + 新增 d1.parquet + f2.parquet         │
│                                                               │
│  优点: ✅ 写入性能高（2KB 写入更新 1 条记录）                    │
│  缺点: ⚠️  读取性能稍低（需合并 data + delete files）           │
└─────────────────────────────────────────────────────────────┘
```

---

## 6. 事务与并发控制

### 6.1 ACID 保证

Iceberg 通过以下机制保证 ACID：

```
Atomicity（原子性）:
  - 元数据文件的原子性更新（metadata.json）
  - 使用文件系统的原子重命名或对象存储的 CAS

Consistency（一致性）:
  - Schema 验证
  - 分区值验证
  - 约束检查

Isolation（隔离性）:
  - Snapshot Isolation（默认）
  - Serializable Isolation（通过冲突检测）

Durability（持久性）:
  - 数据文件持久化
  - 元数据持久化
```

### 6.2 Optimistic Concurrency Control

```java
// 伪代码：Iceberg 的 commit 逻辑

public void commit() {
  while (true) {
    try {
      // 1. 读取当前元数据版本
      TableMetadata current = loadMetadata();

      // 2. 应用变更
      TableMetadata updated = applyChanges(current);

      // 3. 原子性提交（CAS）
      boolean success = compareAndSwap(
        expected: current.version,
        newMetadata: updated
      );

      if (success) {
        return;  // 提交成功
      } else {
        // 4. 冲突检测
        if (hasConflict()) {
          throw new CommitFailedException("Conflict detected");
        }
        // 5. 自动重试
        continue;
      }
    } catch (Exception e) {
      handleError(e);
    }
  }
}
```

### 6.3 冲突检测示例

```scala
// 场景：两个并发的 UPDATE 操作

// Transaction A: UPDATE users SET age = 30 WHERE id = 123
val deltaA = table.newRowDelta()
  .conflictDetectionFilter(Expressions.equal("id", 123))
  .validateNoConflictingDeleteFiles()  // 启用冲突检测
  .addDeletes(deleteFileA)
  .addRows(newDataA)

// Transaction B: UPDATE users SET age = 35 WHERE id = 123
val deltaB = table.newRowDelta()
  .conflictDetectionFilter(Expressions.equal("id", 123))
  .validateNoConflictingDeleteFiles()  // 启用冲突检测
  .addDeletes(deleteFileB)
  .addRows(newDataB)

// 执行时序：
// T1: A 读取 Snapshot 100
// T2: B 读取 Snapshot 100
// T3: A 提交 → Snapshot 101 (包含 deleteFileA)
// T4: B 提交 → 检测到 Snapshot 101 有冲突的 Delete File
//     ↓
//     CommitFailedException: Conflict detected
```

---

## 7. 写入优化

### 7.1 小文件合并（File Compaction）

```scala
// 使用 RewriteFiles API 合并小文件
val rewrite = table.newRewrite()
  .rewriteFiles(
    filesToDelete = Set(small1.parquet, small2.parquet, small3.parquet),
    filesToAdd = Set(merged.parquet)
  )
  .commit()

// 或使用 Spark SQL
spark.sql("CALL iceberg_catalog.system.rewrite_data_files('db.table')")
```

### 7.2 排序写入（Sorted Write）

```scala
// 在表定义中指定 Sort Order
catalog.buildTable(tableId, schema)
  .withSortOrder(
    SortOrder.builderFor(schema)
      .asc("user_id")
      .asc("timestamp")
      .build()
  )
  .create()

// 写入时数据会自动按 Sort Order 排序
// 优势：
// - 提高查询性能（Range Pruning）
// - 提高 Compaction 效率
```

### 7.3 分区策略

```scala
// 分区策略选择

// 1. 时间分区（推荐用于日志/事件数据）
PartitionSpec.builderFor(schema)
  .day("timestamp")  // 按天分区
  .build()

// 2. 哈希分区（推荐用于均匀分布数据）
PartitionSpec.builderFor(schema)
  .bucket("user_id", 16)  // 16 个 Bucket
  .build()

// 3. 分层分区（时间 + 哈希）
PartitionSpec.builderFor(schema)
  .day("timestamp")
  .bucket("user_id", 16)
  .build()
```

---

## 8. 总结

### 8.1 关键要点

1. **Snapshot 是核心**：每次写入都生成新快照，保证不可变性
2. **Manifest 分层**：Manifest List → Manifest File → Data/Delete File
3. **三种写入模式**：
   - Append: 纯追加（INSERT）
   - Overwrite: 重写文件（CoW 模式 UPDATE/DELETE）
   - RowDelta: 增量变更（MoR 模式 UPDATE/DELETE）
4. **乐观并发控制**：通过 CAS + 自动重试保证并发安全
5. **灵活的 Delete File**：Position Delete（精确位置）vs Equality Delete（主键匹配）

### 8.2 最佳实践

| 场景 | 推荐写入方式 | 理由 |
|-----|-------------|------|
| 批量导入 | AppendFiles | 无冲突，性能最高 |
| 定时覆盖分区 | OverwriteFiles | 幂等性好，适合调度任务 |
| 实时 CDC | RowDelta | 写放大小，适合高频更新 |
| OLAP 批量更新 | OverwriteFiles | 读性能优先 |
| OLTP 事务更新 | RowDelta | 写性能优先 |

### 8.3 配置建议

```properties
# 启用 Snapshot ID Inheritance（减少 Manifest 重写）
format-version=2

# 写入模式配置
write.merge.mode=merge-on-read   # MoR 模式
write.update.mode=merge-on-read  # MoR 模式 UPDATE
write.delete.mode=merge-on-read  # MoR 模式 DELETE

# 或使用 CoW 模式
write.merge.mode=copy-on-write
write.update.mode=copy-on-write
write.delete.mode=copy-on-write

# Target File Size（控制文件大小）
write.target-file-size-bytes=536870912  # 512MB
```

---

## 9. 参考资源

- **源码位置**：
  - `api/src/main/java/org/apache/iceberg/AppendFiles.java`
  - `api/src/main/java/org/apache/iceberg/OverwriteFiles.java`
  - `api/src/main/java/org/apache/iceberg/RowDelta.java`
  - `api/src/main/java/org/apache/iceberg/Table.java`

- **相关文档**：
  - `iceberg-read-flow.md` - Iceberg 读取流程
  - `iceberg-merge-into-flow.md` - MERGE INTO 详解

---

**文档版本**: v1.0
**最后更新**: 2025-10-26
**作者**: Claude Code (基于源码分析)
