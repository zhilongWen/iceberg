# Apache Iceberg 源码分析文档

> 基于 Apache Iceberg 1.7.0-SNAPSHOT 源码深度分析
>
> 文档作者：Claude Code
>
> 最后更新：2025-10-26

---

## 📚 文档概览

本目录包含 Apache Iceberg 核心功能的详细技术文档，深入剖析了读写流程和 MERGE INTO 操作的实现原理。

### 文档列表

| 文档 | 主题 | 字数 | 推荐阅读顺序 |
|-----|------|------|------------|
| **iceberg-write-flow.md** | 写入流程详解 | ~8000 | ① 第一篇 |
| **iceberg-read-flow.md** | 读取流程详解 | ~7000 | ② 第二篇 |
| **iceberg-merge-into-flow.md** | MERGE INTO 详解 | ~10000 | ③ 第三篇 |

---

## 📖 阅读指南

### 1. iceberg-write-flow.md - 写入流程详解

**内容概要**：
- AppendFiles：追加写入
- OverwriteFiles：覆盖写入
- RowDelta：行级变更（MoR 模式）
- Copy-on-Write vs Merge-on-Read 对比
- 事务与并发控制
- 写入优化策略

**适合人群**：
- 需要了解 Iceberg 写入机制的开发者
- 优化写入性能的工程师
- 实现 CDC 集成的架构师

**关键概念**：
```
AppendFiles      → INSERT 操作
OverwriteFiles   → UPDATE/DELETE (CoW 模式)
RowDelta         → UPDATE/DELETE (MoR 模式)
Snapshot         → 不可变的表版本
Manifest         → 文件元数据清单
```

---

### 2. iceberg-read-flow.md - 读取流程详解

**内容概要**：
- TableScan：扫描规划
- 分区裁剪（Partition Pruning）
- 文件裁剪（File Pruning）
- 列裁剪（Column Pruning）
- Delete File 合并（MoR 模式）
- Time Travel 与分支读取

**适合人群**：
- 优化查询性能的开发者
- 分析慢查询的工程师
- 设计分区策略的架构师

**关键优化**：
```
分区裁剪 → 跳过整个分区
文件裁剪 → 使用统计信息跳过文件
列裁剪   → 减少 I/O
残余过滤 → 行级过滤
```

---

### 3. iceberg-merge-into-flow.md - MERGE INTO 详解

**内容概要**：
- MERGE INTO 语法与语义
- RewriteMergeIntoTable 改写逻辑
- Copy-on-Write 模式执行流程
- Merge-on-Read 模式执行流程
- MergeRows 执行器
- Delete Files 生成与合并
- Compaction 策略

**适合人群**：
- 实现 UPSERT 逻辑的开发者
- 优化 MERGE INTO 性能的工程师
- 选择 CoW vs MoR 的架构师

**核心对比**：
```
Copy-on-Write:
  ✅ 读取性能高
  ⚠️  写放大严重
  🎯 适合 OLAP

Merge-on-Read:
  ✅ 写入性能高
  ⚠️  读取稍慢
  🎯 适合 OLTP
```

---

## 🎯 快速导航

### 按场景查找

#### 场景1：实现批量数据导入
→ 阅读 **iceberg-write-flow.md** 第 2 节（AppendFiles）

#### 场景2：实现 CDC (Change Data Capture)
→ 阅读 **iceberg-write-flow.md** 第 4 节（RowDelta）
→ 阅读 **iceberg-merge-into-flow.md** 第 5 节（MoR 模式）

#### 场景3：优化慢查询
→ 阅读 **iceberg-read-flow.md** 第 3 节（过滤优化）
→ 阅读 **iceberg-read-flow.md** 第 8 节（性能优化）

#### 场景4：实现 UPSERT 逻辑
→ 阅读 **iceberg-merge-into-flow.md** 第 4 节（CoW 模式）
→ 阅读 **iceberg-merge-into-flow.md** 第 5 节（MoR 模式）

#### 场景5：选择 CoW vs MoR
→ 阅读 **iceberg-write-flow.md** 第 5.2 节（写入流程对比）
→ 阅读 **iceberg-merge-into-flow.md** 第 6 节（CoW vs MoR 对比）

#### 场景6：处理小文件问题
→ 阅读 **iceberg-write-flow.md** 第 7.1 节（小文件合并）
→ 阅读 **iceberg-merge-into-flow.md** 第 9 节（Compaction）

---

## 🔍 核心概念索引

### Snapshot（快照）
- 定义：表的不可变版本
- 相关文档：
  - iceberg-write-flow.md 第 1.1 节
  - iceberg-read-flow.md 第 2.3 节

### Manifest（清单）
- 定义：文件元数据清单（Manifest List → Manifest File → Data/Delete File）
- 相关文档：
  - iceberg-write-flow.md 第 1.1 节
  - iceberg-write-flow.md 第 2.3.2 节（Manifest 复用）

### Partition Pruning（分区裁剪）
- 定义：根据分区列过滤，跳过整个分区
- 相关文档：
  - iceberg-read-flow.md 第 3.2 节
  - iceberg-merge-into-flow.md 第 5.2 节（splitMergeCond）

### File Pruning（文件裁剪）
- 定义：使用统计信息（Min/Max）跳过不相关文件
- 相关文档：
  - iceberg-read-flow.md 第 3.3 节

### Copy-on-Write (CoW)
- 定义：更新时重写整个数据文件
- 相关文档：
  - iceberg-write-flow.md 第 5.2 节
  - iceberg-merge-into-flow.md 第 4 节

### Merge-on-Read (MoR)
- 定义：更新时生成 Delete Files，不重写数据文件
- 相关文档：
  - iceberg-write-flow.md 第 4.2 节（Delete File 类型）
  - iceberg-merge-into-flow.md 第 5 节

### Delete File
- 类型：
  - Position Delete：记录文件路径 + 行号
  - Equality Delete：记录主键值
- 相关文档：
  - iceberg-write-flow.md 第 4.2 节
  - iceberg-read-flow.md 第 5 节
  - iceberg-merge-into-flow.md 第 5.4 节

### MERGE INTO
- 定义：SQL UPSERT 操作（UPDATE + INSERT）
- 相关文档：
  - iceberg-merge-into-flow.md 第 1~10 节（完整文档）

---

## 🛠️ 源码位置索引

### 核心 API 接口
```
api/src/main/java/org/apache/iceberg/
├── Table.java              - 表接口（写入流程 1.1 节）
├── AppendFiles.java        - 追加接口（写入流程 2.1 节）
├── OverwriteFiles.java     - 覆盖接口（写入流程 3.1 节）
├── RowDelta.java           - 行级变更接口（写入流程 4.1 节）
├── DeleteFiles.java        - 删除接口（写入流程）
├── TableScan.java          - 扫描接口（读取流程 2.1 节）
└── Snapshot.java           - 快照接口（写入/读取流程）
```

### Spark 集成实现
```
spark/v3.4/spark-extensions/src/main/scala/org/apache/spark/sql/
├── catalyst/analysis/
│   └── RewriteMergeIntoTable.scala    - MERGE INTO 改写（MERGE INTO 3.1 节）
├── catalyst/plans/logical/
│   ├── MergeIntoIcebergTable.scala    - MERGE INTO 逻辑计划
│   ├── ReplaceIcebergData.scala       - CoW 写入计划
│   └── WriteIcebergDelta.scala        - MoR 写入计划
└── execution/datasources/v2/
    └── MergeRowsExec.scala            - MERGE INTO 物理执行（MERGE INTO 7 节）
```

---

## 📊 图表索引

### 流程图
- **写入流程概览**：iceberg-write-flow.md 第 1.1 节
- **AppendFiles 执行流程**：iceberg-write-flow.md 第 2.2 节
- **OverwriteFiles 执行流程**：iceberg-write-flow.md 第 3.2 节
- **RowDelta 执行流程**：iceberg-write-flow.md 第 4.3 节
- **读取流程分层**：iceberg-read-flow.md 第 1.1 节
- **TableScan 执行流程**：iceberg-read-flow.md 第 2.3 节
- **三级过滤机制**：iceberg-read-flow.md 第 3.1 节
- **Delete File 合并流程**：iceberg-read-flow.md 第 5.1 节
- **MERGE INTO 执行架构**：iceberg-merge-into-flow.md 第 2.1 节
- **CoW 模式执行流程**：iceberg-merge-into-flow.md 第 4.1 节
- **MoR 模式执行流程**：iceberg-merge-into-flow.md 第 5.1 节

### 对比表
- **写入操作分类**：iceberg-write-flow.md 第 1.2 节
- **Append vs Overwrite vs RowDelta**：iceberg-write-flow.md 第 5.1 节
- **CoW vs MoR 特点**：iceberg-write-flow.md 第 5.2 节
- **读取优化关键点**：iceberg-read-flow.md 第 10.2 节
- **CoW vs MoR 性能对比**：iceberg-merge-into-flow.md 第 6.1 节
- **CoW vs MoR 适用场景**：iceberg-merge-into-flow.md 第 6.2 节

---

## 🧪 示例代码位置

本项目包含完整的示例代码，演示了各种写入和读取场景：

```
iceberg-local-spark/src/main/scala/org/apache/iceberg/local/
├── Main.scala                                   - 统一入口
├── SparkIcebergCowSuite.scala                   - CoW 模式验证
├── SparkIcebergMorSuite.scala                   - MoR 模式验证
├── SparkWriteWithPartitionPrimaryKeySuite.scala - MERGE INTO 示例
├── SparkIcebergBranchSuite.scala                - 分支功能演示
└── SparkIcebergTableSchemaEvolutionSuite.scala  - Schema Evolution
```

### 运行示例

```bash
# 验证 CoW 模式（为什么没有 delete 文件）
./gradlew :iceberg-local-spark:run --args="cow-test"

# 验证 MoR 模式（生成 delete 文件）
./gradlew :iceberg-local-spark:run --args="mor-test"

# MERGE INTO 示例（分区表 + 主键）
./gradlew :iceberg-local-spark:run --args="partition"

# Schema Evolution 演示
./gradlew :iceberg-local-spark:run --args="schema-evolution"

# 分支功能演示
./gradlew :iceberg-local-spark:run --args="branch"
```

---

## 💡 最佳实践

### 写入优化
1. **选择合适的写入模式**
   - OLAP 场景 → CoW
   - OLTP 场景 → MoR

2. **控制文件大小**
   ```properties
   write.target-file-size-bytes=536870912  # 512MB
   ```

3. **使用合理的分区策略**
   - 避免过细分区（单个分区太小）
   - 避免过粗分区（单个分区太大）
   - 推荐：每个分区 10-100 个文件

4. **定期执行 Compaction**
   - CoW 模式：合并小文件
   - MoR 模式：清理 Delete Files

### 读取优化
1. **利用分区裁剪**
   ```sql
   -- ✅ 好的查询（利用分区）
   SELECT * FROM events WHERE dt = '2024-10-26'

   -- ❌ 不好的查询（全表扫描）
   SELECT * FROM events WHERE user_id = 123
   ```

2. **列裁剪**
   ```sql
   -- ✅ 只选择需要的列
   SELECT id, name FROM users

   -- ❌ 避免 SELECT *
   SELECT * FROM users
   ```

3. **LIMIT 下推**
   ```scala
   scan.limit(100).planFiles()  // ✅ 下推
   scan.planFiles().take(100)   // ❌ 不下推
   ```

### MERGE INTO 优化
1. **ON 条件包含分区列**
   ```sql
   -- ✅ 利用分区裁剪
   ON target.dt = source.dt AND target.id = source.id

   -- ❌ 全表扫描
   ON target.id = source.id
   ```

2. **避免基数冲突**
   ```sql
   -- 确保 source 中没有重复的主键
   SELECT id, COUNT(*) FROM source GROUP BY id HAVING COUNT(*) > 1
   ```

3. **选择合适的模式**
   - 批量 ETL → CoW
   - 实时 CDC → MoR

---

## 🔗 相关资源

### 官方文档
- [Apache Iceberg 官网](https://iceberg.apache.org/)
- [Iceberg Spec](https://iceberg.apache.org/spec/)
- [Iceberg API](https://iceberg.apache.org/javadoc/)

### 社区资源
- [GitHub Repository](https://github.com/apache/iceberg)
- [Mailing List](https://iceberg.apache.org/community/)
- [Slack Channel](https://apache-iceberg.slack.com/)

### 版本信息
- **Iceberg 版本**: 1.7.0-SNAPSHOT
- **Spark 版本**: 3.4.3
- **Scala 版本**: 2.12.17
- **Flink 版本**: 1.17

---

## 📝 文档维护

### 反馈与贡献
如果您在阅读文档时发现任何问题或有改进建议，欢迎：
1. 提交 Issue
2. 提交 Pull Request
3. 联系文档作者

### 版本历史
- **v1.0** (2025-10-26): 初始版本
  - 完成写入流程文档
  - 完成读取流程文档
  - 完成 MERGE INTO 流程文档

---

**文档版本**: v1.0
**最后更新**: 2025-10-26
**作者**: Claude Code (基于源码分析)
