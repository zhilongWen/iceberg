/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iceberg.local

import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.{PartitionSpec, SortOrder}
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.hadoop.HadoopCatalog
import org.apache.iceberg.spark.SparkSchemaUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, date_format, from_unixtime, lit}

import scala.collection.JavaConverters._

/**
 * 演示：Iceberg 分区表主键唯一性保证
 *
 * 场景：
 *   - 分区字段：dt（日期分区）
 *   - 主键字段：user_id
 *   - 约束：在每个 dt 分区内，user_id 必须唯一
 *
 * 实现方式：
 *   - 使用 MERGE INTO 实现 UPSERT 语义
 *   - ON 条件：target.dt = source.dt AND target.user_id = source.user_id
 *   - WHEN MATCHED：更新现有记录
 *   - WHEN NOT MATCHED：插入新记录
 */
object SparkWriteWithPartitionPrimaryKeySuite {
  def main(args: Array[String]): Unit = {
    // ========== 1. 构建 SparkSession ==========
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName(this.getClass.getSimpleName)
      // 配置 Iceberg Catalog
      .config("spark.sql.catalog.iceberg_local", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.iceberg_local.type", "hadoop")
      .config("spark.sql.catalog.iceberg_local.warehouse", "file:///Users/wenzhilong/warehouse/space/iceberg/files/iceberg_warehouse")
      // ★ 启用 Iceberg Spark Extensions（支持 MERGE INTO）
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .getOrCreate()

    import spark.implicits._

    println("\n" + "=" * 100)
    println("Iceberg 分区表主键唯一性 Demo：dt 分区内 user_id 唯一")
    println("=" * 100)

    // ========== 2. 初始化 Catalog 和表标识 ==========
    val hadoopConf = new Configuration()
    val warehousePath = "file:///Users/wenzhilong/warehouse/space/iceberg/files/iceberg_warehouse"
    val catalog = new HadoopCatalog(hadoopConf, warehousePath)

    val tableIdentifier = TableIdentifier.of("default", "user_behavior_primary_key")
    val fullTableName = "iceberg_local.default.user_behavior_primary_key"

    // ========== 3. 创建表（如果不存在）==========
    if (!catalog.tableExists(tableIdentifier)) {
      println("\n【创建表】")

      // 准备初始数据用于推断 Schema
      val initData = Seq(
        (1L, 101L, "pv", System.currentTimeMillis(), "20251025")
      ).toDF("user_id", "item_id", "behavior", "timestamp", "dt")

      val icebergSchema = SparkSchemaUtil.convert(initData.schema)
      println(s"Schema: ${icebergSchema}")

      // 定义分区规范：按 dt 分区
      val partitionSpec = PartitionSpec
        .builderFor(icebergSchema)
        .identity("dt")
        .build()

      // 定义排序顺序：按主键 user_id 排序
      val sortOrder = SortOrder
        .builderFor(icebergSchema)
        .asc("user_id")
        .asc("timestamp")
        .build()

      // 创建表（设置 primary-key 表属性）
      catalog.buildTable(tableIdentifier, icebergSchema)
        .withPartitionSpec(partitionSpec)
        .withSortOrder(sortOrder)
        .withProperty("primary-key", "dt,user_id")  // 声明主键为 (dt, user_id) 组合
        .create()

      println(s"✓ 表创建成功: $fullTableName")
      println(s"  - 分区字段: dt")
      println(s"  - 主键字段: (dt, user_id) - 通过 primary-key 属性声明")
      println(s"  - 排序字段: user_id ASC, timestamp ASC")
    } else {
      println(s"\n✓ 表已存在: $fullTableName")
    }

    // 查看表属性（验证 primary-key 已设置）
    val table = catalog.loadTable(tableIdentifier)
    val primaryKey = table.properties().get("primary-key")
    println(s"\n【表属性】primary-key = $primaryKey")

    // ========== 4. 第一次写入：初始数据 ==========
    println("\n" + "=" * 100)
    println("【第一次写入】插入初始数据")
    println("=" * 100)

    val batch1 = Seq(
      // dt = 20251024：user_id 1-5
      (1L, 101L, "pv", 1729756800000L, "20251024"),
      (2L, 102L, "buy", 1729756801000L, "20251024"),
      (3L, 103L, "cart", 1729756802000L, "20251024"),
      (4L, 104L, "fav", 1729756803000L, "20251024"),
      (5L, 105L, "pv", 1729756804000L, "20251024"),

      // dt = 20251025：user_id 1-5
      (1L, 201L, "buy", 1729843200000L, "20251025"),
      (2L, 202L, "pv", 1729843201000L, "20251025"),
      (3L, 203L, "cart", 1729843202000L, "20251025"),
      (4L, 204L, "pv", 1729843203000L, "20251025"),
      (5L, 205L, "fav", 1729843204000L, "20251025")
    ).toDF("user_id", "item_id", "behavior", "timestamp", "dt")

    println(s"\n准备写入数据: ${batch1.count()} 条")
    batch1.show(20, false)

    // 使用 DataFrame API 写入
    batch1.writeTo(fullTableName).append()

    // 查询当前数据
    val count1 = spark.sql(s"SELECT COUNT(*) as cnt FROM $fullTableName").collect()(0).getLong(0)
    println(s"\n✓ 第一次写入完成，当前总数: $count1 条")
    println(s"  - 表: $fullTableName 第一次查询总数 ==============================================")
    spark.sql(s"SELECT * FROM $fullTableName").show(1000)

    // 查看各分区数据分布
    println("\n各分区数据分布:")
    spark.sql(
      s"""
         |SELECT dt, COUNT(*) as count,
         |       MIN(user_id) as min_user_id,
         |       MAX(user_id) as max_user_id
         |FROM $fullTableName
         |GROUP BY dt
         |ORDER BY dt
         |""".stripMargin
    ).show()

    // ========== 5. 第二次写入：使用 MERGE INTO 更新 + 插入 ==========
    println("\n" + "=" * 100)
    println("【第二次写入】使用 MERGE INTO 实现 UPSERT（保证分区内主键唯一）")
    println("=" * 100)

    val batch2 = Seq(
      // dt = 20251024：user_id 3-7
      //   - user_id 3,4,5 已存在 → UPDATE
      //   - user_id 6,7 不存在 → INSERT
      (3L, 9103L, "buy", 1729756902000L, "20251024"),    // UPDATE
      (4L, 9104L, "pv", 1729756903000L, "20251024"),     // UPDATE
      (5L, 9105L, "cart", 1729756904000L, "20251024"),   // UPDATE
      (6L, 106L, "fav", 1729756905000L, "20251024"),     // INSERT
      (7L, 107L, "pv", 1729756906000L, "20251024"),      // INSERT

      // dt = 20251025：user_id 4-8
      //   - user_id 4,5 已存在 → UPDATE
      //   - user_id 6,7,8 不存在 → INSERT
      (4L, 9204L, "buy", 1729843303000L, "20251025"),    // UPDATE
      (5L, 9205L, "cart", 1729843304000L, "20251025"),   // UPDATE
      (6L, 206L, "pv", 1729843305000L, "20251025"),      // INSERT
      (7L, 207L, "fav", 1729843306000L, "20251025"),     // INSERT
      (8L, 208L, "buy", 1729843307000L, "20251025"),     // INSERT

      // dt = 20251026：全新分区
      //   - user_id 1-3 全部 INSERT
      (1L, 301L, "pv", 1729929600000L, "20251026"),      // INSERT
      (2L, 302L, "buy", 1729929601000L, "20251026"),     // INSERT
      (3L, 303L, "cart", 1729929602000L, "20251026")     // INSERT
    ).toDF("user_id", "item_id", "behavior", "timestamp", "dt")

    println(s"\n准备 MERGE 数据: ${batch2.count()} 条")
    println("预期效果：")
    println("  - dt=20251024: user_id 3,4,5 更新，6,7 新增")
    println("  - dt=20251025: user_id 4,5 更新，6,7,8 新增")
    println("  - dt=20251026: user_id 1,2,3 新增（新分区）")

    batch2.show(20, false)

    // 创建临时视图
    batch2.createOrReplaceTempView("source_data")

    // 查询 MERGE 前的状态
    val beforeMerge = spark.sql(s"SELECT COUNT(*) as cnt FROM $fullTableName").collect()(0).getLong(0)
    println(s"\n【MERGE 前】总记录数: $beforeMerge")

    // 执行 MERGE INTO
    println("\n执行 MERGE INTO...")
    spark.sql(
      s"""
         |MERGE INTO $fullTableName AS target
         |USING source_data AS source
         |ON target.dt = source.dt AND target.user_id = source.user_id
         |WHEN MATCHED THEN
         |  UPDATE SET
         |    target.item_id = source.item_id,
         |    target.behavior = source.behavior,
         |    target.timestamp = source.timestamp
         |WHEN NOT MATCHED THEN
         |  INSERT (user_id, item_id, behavior, timestamp, dt)
         |  VALUES (source.user_id, source.item_id, source.behavior, source.timestamp, source.dt)
         |""".stripMargin
    )

    // 查询 MERGE 后的状态
    val afterMerge = spark.sql(s"SELECT COUNT(*) as cnt FROM $fullTableName").collect()(0).getLong(0)
    println(s"\n✓ MERGE INTO 完成！")
    println(s"【MERGE 后】总记录数: $afterMerge")
    println(s"【变化】新增记录: ${afterMerge - beforeMerge} 条（预期 8 条：dt=20251024 新增2条，dt=20251025 新增3条，dt=20251026 新增3条）")
    println(s"  - 表: $fullTableName 第二次查询总数 ==============================================")
    spark.sql(s"SELECT * FROM $fullTableName").show(1000)

    // 查看各分区数据分布
    println("\n各分区数据分布:")
    spark.sql(
      s"""
         |SELECT dt, COUNT(*) as count,
         |       MIN(user_id) as min_user_id,
         |       MAX(user_id) as max_user_id
         |FROM $fullTableName
         |GROUP BY dt
         |ORDER BY dt
         |""".stripMargin
    ).show()

    // ========== 6. 验证主键唯一性 ==========
    println("\n" + "=" * 100)
    println("【验证主键唯一性】检查 dt 分区内是否存在重复的 user_id")
    println("=" * 100)

    val duplicateCheck = spark.sql(
      s"""
         |SELECT dt, user_id, COUNT(*) as cnt
         |FROM $fullTableName
         |GROUP BY dt, user_id
         |HAVING COUNT(*) > 1
         |""".stripMargin
    )

    val duplicateCount = duplicateCheck.count()
    if (duplicateCount == 0) {
      println("✓ 验证通过！所有分区内的 user_id 都是唯一的")
    } else {
      println(s"✗ 验证失败！发现 $duplicateCount 组重复的 (dt, user_id)：")
      duplicateCheck.show(false)
    }

    // ========== 7. 验证更新是否生效 ==========
    println("\n" + "=" * 100)
    println("【验证更新】查看被更新的记录")
    println("=" * 100)

    println("\ndt=20251024 的数据（user_id 3,4,5 的 item_id 应该是 91xx）：")
    spark.sql(
      s"""
         |SELECT dt, user_id, item_id, behavior, timestamp
         |FROM $fullTableName
         |WHERE dt = '20251024'
         |ORDER BY user_id
         |""".stripMargin
    ).show()

    println("\ndt=20251025 的数据（user_id 4,5 的 item_id 应该是 92xx）：")
    spark.sql(
      s"""
         |SELECT dt, user_id, item_id, behavior, timestamp
         |FROM $fullTableName
         |WHERE dt = '20251025'
         |ORDER BY user_id
         |""".stripMargin
    ).show()

    println("\ndt=20251026 的数据（全新分区）：")
    spark.sql(
      s"""
         |SELECT dt, user_id, item_id, behavior, timestamp
         |FROM $fullTableName
         |WHERE dt = '20251026'
         |ORDER BY user_id
         |""".stripMargin
    ).show()

    // ========== 8. 查看完整数据 ==========
    println("\n" + "=" * 100)
    println("【完整数据】")
    println("=" * 100)

    spark.sql(
      s"""
         |SELECT dt, user_id, item_id, behavior, timestamp
         |FROM $fullTableName
         |ORDER BY dt, user_id
         |""".stripMargin
    ).show(50)

    // ========== 9. 总结 ==========
    println("\n" + "=" * 100)
    println("【总结】")
    println("=" * 100)
    println("✓ 通过表属性 'primary-key' 声明了主键为 (dt, user_id)")
    println("✓ MERGE INTO 成功保证了 dt 分区内 user_id 的唯一性")
    println("✓ 相同 (dt, user_id) 的记录被更新，不同的记录被插入")
    println("✓ 适用场景：CDC 数据同步、增量数据更新、主键去重等")
    println("\n【注意】")
    println("- primary-key 属性是元数据标记，不会自动强制约束")
    println("- 需要通过 MERGE INTO 在应用层保证主键唯一性")
    println("- Flink CDC 等流式引擎会利用 primary-key 属性自动执行 UPSERT")

    spark.stop()
  }
}
