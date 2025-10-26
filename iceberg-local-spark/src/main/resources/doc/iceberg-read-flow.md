# Apache Iceberg 读取流程详解

> 基于 Apache Iceberg 1.7.0-SNAPSHOT 源码分析
>
> 文档目标：深入理解 Iceberg 的读取机制，包括 TableScan、文件裁剪、列裁剪等优化策略

---

## 1. 架构概览

### 1.1 读取流程分层

```
┌────────────────────────────────────────────────────────────────┐
│                   Iceberg 读取流程分层                           │
└────────────────────────────────────────────────────────────────┘

查询引擎（Spark/Flink/Presto）
    ↓
┌──────────────────────────────┐
│  1. Table Scan Planning      │ ← TableScan API
│  - 读取 Snapshot             │
│  - 应用过滤条件（Filter）      │
│  - 执行分区裁剪               │
│  - 执行文件裁剪               │
└──────────────────────────────┘
    ↓
┌──────────────────────────────┐
│  2. Task Planning            │ ← FileScanTask
│  - 生成读取任务               │
│  - 合并小任务（可选）          │
│  - 分配给执行器               │
└──────────────────────────────┘
    ↓
┌──────────────────────────────┐
│  3. File Reading             │ ← FileIO + InputFile
│  - 读取数据文件               │
│  - 应用列裁剪                 │
│  - 应用残余过滤（Residual）    │
│  - 合并 Delete Files          │
└──────────────────────────────┘
    ↓
┌──────────────────────────────┐
│  4. Row Filtering            │ ← Expression Evaluation
│  - 行级过滤                   │
│  - Schema Evolution 处理      │
│  - 类型转换                   │
└──────────────────────────────┘
    ↓
返回结果集
```

### 1.2 核心概念

| 概念 | 说明 | 示例 |
|-----|------|------|
| **Snapshot** | 表的不可变版本 | Snapshot ID: 123456 |
| **Manifest List** | 指向所有 Manifest Files | manifest-list-xxx.avro |
| **Manifest File** | 包含数据文件元信息 | manifest-xxx.avro |
| **DataFile** | 实际数据文件 | data/part1/file1.parquet |
| **DeleteFile** | 删除文件（MoR 模式） | delete/part1/delete1.parquet |

---

## 2. TableScan：扫描规划

### 2.1 API 定义

**文件路径**: `api/src/main/java/org/apache/iceberg/TableScan.java`

```java
public interface TableScan extends Scan<TableScan, FileScanTask, CombinedScanTask> {
  /**
   * Returns the {@link Table} from which this scan loads data.
   */
  Table table();

  /**
   * Create a new scan for a specific snapshot by ID.
   */
  TableScan useSnapshot(long snapshotId);

  /**
   * Create a new scan for a specific branch.
   */
  TableScan useRef(String ref);

  /**
   * Create a new scan for a specific point in time.
   *
   * Time Travel 查询
   */
  TableScan asOfTime(long timestampMillis);

  /**
   * Returns the {@link Snapshot} that will be used by this scan.
   */
  Snapshot snapshot();
}
```

### 2.2 Scan 接口（通用扫描）

```java
public interface Scan<ThisT, T extends ScanTask, G extends ScanTaskGroup<T>> {
  /**
   * Create a new scan with a filter Expression.
   *
   * 添加过滤条件
   */
  ThisT filter(Expression expr);

  /**
   * Create a new scan to read only specified columns.
   *
   * 列裁剪
   */
  ThisT select(Collection<String> columns);

  /**
   * Create a new scan with a limit.
   *
   * LIMIT 下推
   */
  ThisT limit(long limit);

  /**
   * Plan the scan tasks.
   *
   * 生成扫描任务
   */
  CloseableIterable<T> planFiles();

  /**
   * Plan combined scan tasks.
   *
   * 合并小任务
   */
  CloseableIterable<G> planTasks();
}
```

### 2.3 执行流程

```
┌──────────────────────────────────────────────────────────────────┐
│                    TableScan 执行流程                              │
└──────────────────────────────────────────────────────────────────┘

1. 创建 TableScan
   ↓
   TableScan scan = table.newScan()
   ↓
2. 配置扫描参数
   ↓
   scan = scan
     .filter(Expressions.equal("status", "active"))  // 过滤条件
     .select("id", "name", "age")                    // 列裁剪
     .useSnapshot(snapshotId)                        // 指定快照
   ↓
3. 执行扫描规划
   ↓
   CloseableIterable<FileScanTask> tasks = scan.planFiles()
   ↓
   ┌─────────────────────────────────────┐
   │  Iceberg 内部优化                     │
   ├─────────────────────────────────────┤
   │ 3.1 读取 Snapshot Metadata           │
   │     ↓                                │
   │     读取 manifest-list-xxx.avro      │
   │     解析出所有 Manifest Files         │
   │                                      │
   │ 3.2 分区裁剪（Partition Pruning）     │
   │     ↓                                │
   │     根据过滤条件，跳过不相关的分区       │
   │     例如: WHERE dt = '2024-10-26'    │
   │           → 只读取 dt=2024-10-26 分区 │
   │                                      │
   │ 3.3 读取相关 Manifest Files           │
   │     ↓                                │
   │     只读取包含相关分区的 Manifest      │
   │                                      │
   │ 3.4 文件裁剪（File Pruning）          │
   │     ↓                                │
   │     根据 Manifest 中的统计信息：       │
   │     - Min/Max 值                     │
   │     - Null Count                     │
   │     - Column Bounds                  │
   │     跳过不匹配的数据文件                │
   │                                      │
   │ 3.5 生成 FileScanTask               │
   │     ↓                                │
   │     为每个需要读取的数据文件生成任务    │
   │     包含：                            │
   │     - 文件路径                        │
   │     - 列裁剪信息                      │
   │     - 残余过滤表达式（Residual）       │
   │     - 关联的 Delete Files            │
   └─────────────────────────────────────┘
   ↓
4. 返回扫描任务
   遍历 FileScanTask 并读取数据
```

---

## 3. 过滤优化

### 3.1 三级过滤机制

```
┌────────────────────────────────────────────────────────────────┐
│                  Iceberg 三级过滤机制                            │
└────────────────────────────────────────────────────────────────┘

查询: SELECT * FROM users WHERE age > 25 AND city = 'Beijing'

┌─────────────────────────────────┐
│  Level 1: 分区裁剪                │
│  (Partition Pruning)             │
├─────────────────────────────────┤
│  如果表按 city 分区：              │
│  ↓                               │
│  只扫描 city=Beijing 分区          │
│  跳过其他分区（city=Shanghai 等）   │
└─────────────────────────────────┘
        ↓
┌─────────────────────────────────┐
│  Level 2: 文件裁剪                │
│  (File Pruning)                  │
├─────────────────────────────────┤
│  使用 Manifest 中的统计信息：       │
│  ↓                               │
│  File1: age range [20, 30]       │
│    → 可能包含 age > 25，保留       │
│  File2: age range [10, 20]       │
│    → 全部 < 25，跳过              │
│  File3: age range [30, 40]       │
│    → 全部 > 25，保留              │
└─────────────────────────────────┘
        ↓
┌─────────────────────────────────┐
│  Level 3: 行级过滤                │
│  (Row-Level Filtering)           │
├─────────────────────────────────┤
│  读取保留的文件，逐行判断：          │
│  ↓                               │
│  Row1: age=28 → 保留              │
│  Row2: age=24 → 丢弃              │
│  Row3: age=32 → 保留              │
└─────────────────────────────────┘
        ↓
    返回结果
```

### 3.2 分区裁剪（Partition Pruning）

```scala
// 示例：按时间分区的表
catalog.buildTable(tableId, schema)
  .withPartitionSpec(
    PartitionSpec.builderFor(schema)
      .day("timestamp")
      .build()
  )
  .create()

// 查询：
// SELECT * FROM events WHERE timestamp >= '2024-10-25'
//
// 分区裁剪逻辑：
// 1. 解析过滤条件: timestamp >= '2024-10-25'
// 2. 计算分区值: day(timestamp) = 20241025, 20241026, ...
// 3. 只读取这些分区的 Manifest Files
// 4. 跳过 day < 20241025 的所有分区

优势：
  ✅ 大幅减少读取的 Manifest Files
  ✅ 跳过整个分区目录
  ✅ 降低 I/O 和网络开销
```

### 3.3 文件裁剪（File Pruning）

```java
// Manifest File 中存储的文件统计信息

public interface DataFile {
  // 文件路径
  String path();

  // 行数
  long recordCount();

  // 文件大小
  long fileSizeInBytes();

  // 列统计信息
  Map<Integer, Long> valueCounts();     // 每列的非空值数量
  Map<Integer, Long> nullValueCounts(); // 每列的 NULL 值数量
  Map<Integer, Long> nanValueCounts();  // 每列的 NaN 值数量
  Map<Integer, ByteBuffer> lowerBounds();  // 每列的最小值
  Map<Integer, ByteBuffer> upperBounds();  // 每列的最大值
}

// 文件裁剪示例
// 查询: SELECT * FROM users WHERE age > 50

Manifest 信息：
  File1: age range [20, 40]  → max < 50，跳过
  File2: age range [35, 65]  → 可能包含 > 50 的数据，保留
  File3: age range [60, 80]  → min > 50，保留

结果：只读取 File2 和 File3
```

### 3.4 残余过滤（Residual Filtering）

```
当无法完全通过分区/文件裁剪过滤时，生成残余过滤表达式

示例：
  原始过滤: age > 25 AND city = 'Beijing'
  分区列: city

  分区裁剪后:
    - 已确定 city = 'Beijing'（通过分区裁剪）
    - 残余表达式: age > 25（需要行级过滤）

FileScanTask 包含残余表达式：
  task.residual() → age > 25

读取文件时应用残余过滤：
  ParquetReader.builder(file)
    .withFilter(residual)  // 下推到 Parquet
    .build()
```

---

## 4. FileScanTask：文件扫描任务

### 4.1 任务结构

```java
public interface FileScanTask extends ScanTask {
  /**
   * Returns the {@link DataFile} to scan.
   */
  DataFile file();

  /**
   * Returns the {@link DeleteFile}s to apply.
   *
   * MoR 模式：需要合并 Delete Files
   */
  List<DeleteFile> deletes();

  /**
   * Returns the starting position offset for this task.
   *
   * 支持文件分片（Split）
   */
  long start();

  /**
   * Returns the number of bytes to scan from the start position.
   */
  long length();

  /**
   * Returns the residual expression that should be applied.
   *
   * 残余过滤表达式
   */
  Expression residual();
}
```

### 4.2 任务合并（Task Combining）

```
小文件问题：
  如果每个文件都生成一个任务，会产生大量小任务
  ↓
  解决方案：合并小任务

scan.planTasks() 会自动合并：
  ┌────────────────────────────────────┐
  │  Before Combining                  │
  ├────────────────────────────────────┤
  │  Task1: File1 (10MB)               │
  │  Task2: File2 (5MB)                │
  │  Task3: File3 (8MB)                │
  │  Task4: File4 (12MB)               │
  │  ...                               │
  │  Total: 100 tasks                  │
  └────────────────────────────────────┘
         ↓ 合并
  ┌────────────────────────────────────┐
  │  After Combining                   │
  ├────────────────────────────────────┤
  │  CombinedTask1:                    │
  │    - File1 (10MB)                  │
  │    - File2 (5MB)                   │
  │    - File3 (8MB)                   │
  │    Total: 23MB                     │
  │                                    │
  │  CombinedTask2:                    │
  │    - File4 (12MB)                  │
  │    - File5 (10MB)                  │
  │    Total: 22MB                     │
  │  ...                               │
  │  Total: 10 combined tasks          │
  └────────────────────────────────────┘

优势：
  ✅ 减少任务调度开销
  ✅ 提高并行度
  ✅ 更好的资源利用
```

---

## 5. Delete File 合并（MoR 模式）

### 5.1 读取流程

```
MoR 模式：读取时需要合并 Data File 和 Delete File

┌──────────────────────────────────────────────────────────────────┐
│                Delete File 合并流程                                │
└──────────────────────────────────────────────────────────────────┘

1. 扫描规划
   ↓
   scan.planFiles()
   ↓
   生成 FileScanTask:
     - dataFile: data/f1.parquet
     - deleteFiles: [delete/d1.parquet, delete/d2.parquet]
   ↓
2. 读取 Data File
   ↓
   读取 data/f1.parquet → 1000 rows
   ↓
3. 读取 Delete Files
   ↓
   Delete File d1.parquet (Position Delete):
     ┌──────────────────┬─────┐
     │ file_path        │ pos │
     ├──────────────────┼─────┤
     │ data/f1.parquet  │ 10  │
     │ data/f1.parquet  │ 25  │
     └──────────────────┴─────┘

   Delete File d2.parquet (Equality Delete):
     ┌────┐
     │ id │
     ├────┤
     │ 99 │
     └────┘
   ↓
4. 合并逻辑
   ↓
   遍历 Data File 的每一行：
     - Row 10: 在 Position Delete 中 → 跳过
     - Row 25: 在 Position Delete 中 → 跳过
     - Row 100 (id=99): 在 Equality Delete 中 → 跳过
     - 其他行: 保留
   ↓
5. 返回结果
   返回 997 行（1000 - 3）
```

### 5.2 Delete File 类型处理

```java
// Position Delete: 按文件路径 + 行号删除
// 实现逻辑：
Map<String, Set<Long>> positionDeletes = new HashMap<>();
for (DeleteFile deleteFile : task.deletes()) {
  if (deleteFile.content() == FileContent.POSITION_DELETES) {
    // 读取 delete file
    for (PositionDelete<T> delete : readPositionDeletes(deleteFile)) {
      positionDeletes
        .computeIfAbsent(delete.path(), k -> new HashSet<>())
        .add(delete.pos());
    }
  }
}

// 读取 data file 时过滤
long currentPos = 0;
for (T row : readDataFile(task.file())) {
  if (!positionDeletes.get(task.file().path()).contains(currentPos)) {
    yield(row);  // 保留行
  }
  currentPos++;
}

// Equality Delete: 按主键值删除
// 实现逻辑：
Set<Object> equalityDeletes = new HashSet<>();
for (DeleteFile deleteFile : task.deletes()) {
  if (deleteFile.content() == FileContent.EQUALITY_DELETES) {
    // 读取 delete file（假设主键是 id）
    for (EqualityDelete delete : readEqualityDeletes(deleteFile)) {
      equalityDeletes.add(delete.get("id"));
    }
  }
}

// 读取 data file 时过滤
for (T row : readDataFile(task.file())) {
  if (!equalityDeletes.contains(row.get("id"))) {
    yield(row);  // 保留行
  }
}
```

---

## 6. 列裁剪（Column Pruning）

### 6.1 Schema Projection

```scala
// 原始 Schema
Schema originalSchema = new Schema(
  required(1, "id", Types.IntegerType.get()),
  optional(2, "name", Types.StringType.get()),
  optional(3, "age", Types.IntegerType.get()),
  optional(4, "address", Types.StringType.get())
)

// 查询：SELECT id, name FROM users
scan = scan.select("id", "name")

// 生成 Projected Schema
Schema projectedSchema = new Schema(
  required(1, "id", Types.IntegerType.get()),
  optional(2, "name", Types.StringType.get())
)

// 文件读取时只读取这两列
ParquetReader.builder(file)
  .withProjectedSchema(projectedSchema)
  .build()

优势：
  ✅ 减少 I/O（跳过 age 和 address 列）
  ✅ 减少内存占用
  ✅ 提高网络传输效率（对于远程存储）
```

### 6.2 嵌套列裁剪

```scala
// 嵌套 Schema
Schema nestedSchema = new Schema(
  required(1, "id", Types.IntegerType.get()),
  optional(2, "profile", Types.StructType.of(
    required(3, "name", Types.StringType.get()),
    optional(4, "age", Types.IntegerType.get()),
    optional(5, "address", Types.StructType.of(
      required(6, "city", Types.StringType.get()),
      optional(7, "street", Types.StringType.get())
    ))
  ))
)

// 查询：SELECT id, profile.name, profile.address.city FROM users
scan = scan.select("id", "profile.name", "profile.address.city")

// 生成 Projected Schema
Schema projectedSchema = new Schema(
  required(1, "id", Types.IntegerType.get()),
  optional(2, "profile", Types.StructType.of(
    required(3, "name", Types.StringType.get()),
    optional(5, "address", Types.StructType.of(
      required(6, "city", Types.StringType.get())
      // street 被裁剪
    ))
    // age 被裁剪
  ))
)

优势：
  ✅ 对于复杂嵌套结构，列裁剪效果更显著
```

---

## 7. Time Travel 与分支读取

### 7.1 Time Travel

```scala
// 方式1：按快照 ID 读取
val snapshotId = 123456L
scan = table.newScan().useSnapshot(snapshotId)

// 方式2：按时间戳读取
val timestamp = System.currentTimeMillis() - 3600000L  // 1 小时前
scan = table.newScan().asOfTime(timestamp)

// Spark SQL 语法
spark.sql("""
  SELECT * FROM iceberg_catalog.db.users
  VERSION AS OF 123456
""")

spark.sql("""
  SELECT * FROM iceberg_catalog.db.users
  TIMESTAMP AS OF '2024-10-26 10:00:00'
""")

实现逻辑：
  1. 根据 snapshotId 或 timestamp 定位 Snapshot
  2. 读取该 Snapshot 的 Manifest List
  3. 后续流程与当前快照读取一致
```

### 7.2 分支读取

```scala
// 读取 dev 分支
scan = table.newScan().useRef("dev")

// Spark SQL 语法
spark.sql("""
  SELECT * FROM iceberg_catalog.db.users.branch_dev
""")

实现逻辑：
  1. 从 Table Metadata 中查找分支 "dev"
  2. 获取分支指向的 Snapshot ID
  3. 使用该 Snapshot 进行扫描
```

---

## 8. 读取性能优化

### 8.1 统计信息更新

```scala
// 确保 Manifest 包含准确的统计信息
// 写入时自动生成，但可以手动更新

// 使用 Spark SQL 重写统计信息
spark.sql("""
  CALL iceberg_catalog.system.rewrite_manifests('db.users')
""")

// 更新列统计信息
spark.sql("""
  CALL iceberg_catalog.system.rewrite_data_files(
    table => 'db.users',
    options => map('rewrite-all', 'true')
  )
""")
```

### 8.2 文件大小控制

```properties
# 推荐的文件大小配置
write.target-file-size-bytes=536870912  # 512MB

# 避免小文件问题
# 小文件会导致：
# - 大量 FileScanTask
# - 高调度开销
# - 低并行度

# 避免大文件问题
# 大文件会导致：
# - 文件裁剪效果差
# - 内存占用高
# - 任务不均衡
```

### 8.3 分区策略

```scala
// 分区粒度选择

// ❌ 过细分区（小时级）
PartitionSpec.builderFor(schema)
  .hour("timestamp")  // 可能产生数万个分区
  .build()

// ✅ 合理分区（天级）
PartitionSpec.builderFor(schema)
  .day("timestamp")   // 平衡扫描效率和文件数量
  .build()

// ✅ 分层分区（天 + Bucket）
PartitionSpec.builderFor(schema)
  .day("timestamp")
  .bucket("user_id", 16)  // 进一步细分
  .build()

建议：
  - 每个分区包含 10-100 个文件
  - 每个文件 128MB - 512MB
  - 避免超过 10,000 个分区
```

---

## 9. Spark 集成示例

### 9.1 基本读取

```scala
// 使用 Spark SQL
spark.sql("SELECT * FROM iceberg_catalog.db.users WHERE age > 25")

// 使用 DataFrame API
spark.read
  .format("iceberg")
  .load("iceberg_catalog.db.users")
  .filter("age > 25")
  .select("id", "name")

// 底层执行流程
TableScan scan = table.newScan()
  .filter(Expressions.greaterThan("age", 25))
  .select("id", "name")
  .planFiles()
```

### 9.2 Time Travel

```scala
// 读取历史版本
spark.read
  .format("iceberg")
  .option("snapshot-id", "123456")
  .load("iceberg_catalog.db.users")

// 读取 1 小时前的数据
spark.read
  .format("iceberg")
  .option("as-of-timestamp", System.currentTimeMillis() - 3600000)
  .load("iceberg_catalog.db.users")
```

### 9.3 分支读取

```scala
// 读取 dev 分支
spark.read
  .format("iceberg")
  .option("branch", "dev")
  .load("iceberg_catalog.db.users")

// 或使用 SQL
spark.sql("SELECT * FROM iceberg_catalog.db.users.branch_dev")
```

---

## 10. 读取流程总结

### 10.1 完整流程图

```
查询：SELECT id, name FROM users WHERE age > 25 AND city = 'Beijing'

┌─────────────────────────────────────────────────────────────────┐
│  1. Table Scan 创建                                              │
│     scan = table.newScan()                                      │
│       .filter(and(greaterThan("age", 25), equal("city", "Beijing")))│
│       .select("id", "name")                                     │
└─────────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────────┐
│  2. 读取 Snapshot Metadata                                       │
│     - 当前快照 ID: 456789                                        │
│     - Manifest List: manifest-list-xxx.avro                     │
└─────────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────────┐
│  3. 分区裁剪（Partition Pruning）                                 │
│     - 分区列: city                                               │
│     - 过滤条件: city = 'Beijing'                                 │
│     - 保留分区: city=Beijing (10 个 Manifest Files)              │
│     - 跳过分区: city=Shanghai, city=Guangzhou, ...              │
└─────────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────────┐
│  4. 读取 Manifest Files                                          │
│     - 读取 10 个 Manifest Files                                  │
│     - 包含 200 个 DataFile 条目                                  │
└─────────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────────┐
│  5. 文件裁剪（File Pruning）                                      │
│     - 使用 age > 25 过滤                                         │
│     - File1: age range [20, 30] → 保留                          │
│     - File2: age range [10, 20] → 跳过（max < 25）              │
│     - ...                                                       │
│     - 保留 120 个文件，跳过 80 个文件                              │
└─────────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────────┐
│  6. 生成 FileScanTask                                            │
│     - 120 个 FileScanTask                                        │
│     - 每个包含：                                                  │
│       - DataFile: data/city=Beijing/xxx.parquet                 │
│       - Residual: age > 25                                      │
│       - Schema Projection: [id, name]                           │
│       - DeleteFiles: [delete/xxx.parquet] (如果有)               │
└─────────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────────┐
│  7. 任务合并（可选）                                              │
│     - planTasks() → 合并为 12 个 CombinedScanTask               │
└─────────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────────┐
│  8. 并行读取文件                                                  │
│     - Spark Executor 并行执行 12 个任务                           │
│     - 每个任务：                                                  │
│       - 读取 Parquet 文件（只读 id, name 列）                     │
│       - 应用残余过滤（age > 25）                                  │
│       - 合并 Delete Files（如果有）                               │
└─────────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────────┐
│  9. 返回结果                                                      │
│     - 满足条件的所有行                                            │
└─────────────────────────────────────────────────────────────────┘
```

### 10.2 关键优化点

| 优化点 | 机制 | 效果 |
|-------|------|------|
| **分区裁剪** | 根据分区列过滤 | 跳过整个分区目录 |
| **文件裁剪** | 使用 Min/Max 统计信息 | 跳过不相关的文件 |
| **列裁剪** | Schema Projection | 减少 I/O |
| **残余过滤** | 下推到文件格式 | 减少内存占用 |
| **任务合并** | 合并小文件任务 | 减少调度开销 |
| **Delete File 合并** | 逐行过滤（MoR） | 保证数据一致性 |

### 10.3 性能对比

```
示例表：
  - 总数据量: 1TB
  - 分区: 365 天（按天分区）
  - 文件数: 10,000 个

查询: SELECT * FROM events WHERE dt = '2024-10-26' AND status = 'active'

┌────────────────────────────────────────────────────────────┐
│  无优化（全表扫描）                                           │
├────────────────────────────────────────────────────────────┤
│  - 读取分区: 365 个                                          │
│  - 读取文件: 10,000 个                                       │
│  - 数据量: 1TB                                              │
│  - 耗时: ~10 分钟                                            │
└────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────┐
│  分区裁剪                                                     │
├────────────────────────────────────────────────────────────┤
│  - 读取分区: 1 个 (dt=2024-10-26)                            │
│  - 读取文件: 28 个                                           │
│  - 数据量: ~2.8GB                                           │
│  - 耗时: ~30 秒                                             │
└────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────┐
│  分区裁剪 + 文件裁剪                                          │
├────────────────────────────────────────────────────────────┤
│  - 读取分区: 1 个                                            │
│  - 读取文件: 8 个 (status=active 的文件)                     │
│  - 数据量: ~800MB                                           │
│  - 耗时: ~10 秒                                             │
└────────────────────────────────────────────────────────────┘

性能提升：60 倍！
```

---

## 11. 最佳实践

### 11.1 表设计建议

1. **合理的分区策略**
   - 根据查询模式选择分区列
   - 避免过细分区（单个分区太小）
   - 避免过粗分区（单个分区太大）

2. **合适的文件大小**
   - 目标文件大小：128MB - 512MB
   - 定期执行 Compaction 合并小文件

3. **启用统计信息**
   - format-version=2（支持更丰富的统计信息）
   - 定期更新 Manifest 统计信息

### 11.2 查询优化建议

1. **利用分区列**
   ```sql
   -- ✅ 好的查询（利用分区）
   SELECT * FROM events WHERE dt = '2024-10-26' AND status = 'active'

   -- ❌ 不好的查询（全表扫描）
   SELECT * FROM events WHERE user_id = 123
   ```

2. **列裁剪**
   ```sql
   -- ✅ 好的查询（只选择需要的列）
   SELECT id, name FROM users WHERE age > 25

   -- ❌ 不好的查询（读取所有列）
   SELECT * FROM users WHERE age > 25
   ```

3. **LIMIT 下推**
   ```scala
   // ✅ 利用 LIMIT 下推
   scan.limit(100).planFiles()  // 只读取必要的文件

   // ❌ 不下推 LIMIT
   scan.planFiles().take(100)   // 可能读取所有文件
   ```

---

## 12. 参考资源

- **源码位置**：
  - `api/src/main/java/org/apache/iceberg/TableScan.java`
  - `api/src/main/java/org/apache/iceberg/FileScanTask.java`
  - `api/src/main/java/org/apache/iceberg/Snapshot.java`

- **相关文档**：
  - `iceberg-write-flow.md` - Iceberg 写入流程
  - `iceberg-merge-into-flow.md` - MERGE INTO 详解

---

**文档版本**: v1.0
**最后更新**: 2025-10-26
**作者**: Claude Code (基于源码分析)
