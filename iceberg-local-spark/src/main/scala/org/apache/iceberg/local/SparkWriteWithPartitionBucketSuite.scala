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
import org.apache.iceberg.{PartitionSpec, Schema, SortOrder}
import org.apache.iceberg.catalog.{Namespace, TableIdentifier}
import org.apache.iceberg.hadoop.HadoopCatalog
import org.apache.iceberg.spark.SparkSchemaUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, date_format, from_unixtime}

import scala.collection.JavaConverters._

object SparkWriteWithPartitionBucketSuite {
  def main(args: Array[String]): Unit = {
    // 构建SparkSession，配置本地Iceberg
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]") // 使用本地模式
      .appName(this.getClass.getSimpleName)
      // 配置本地Hadoop catalog（存储在本地文件系统）
      .config("spark.sql.catalog.iceberg_local", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.iceberg_local.type", "hadoop")
      .config("spark.sql.catalog.iceberg_local.warehouse", "file:///Users/wenzhilong/warehouse/space/iceberg/files/iceberg_warehouse")
      .getOrCreate()

    spark.sql("select 1 as id, 'a' as data").show()

    import spark.implicits._

    def builder_behavior(idx: Int): String = {
      idx % 4 match {
        case 0 => "pv"
        case 1 => "buy"
        case 2 => "cart"
        case 3 => "fav"
      }
    }

    // 生成用户行为数据
    val sourceData = Range(1, 100).map { idx =>
      UserBehavior(
        user_id = idx,
        item_id = idx * 100 + idx,
        behavior = builder_behavior(idx),
        timestamp = System.currentTimeMillis()
      )
    }.toDF()


    // 添加 dt 列（日期分区字段）
    val df = sourceData.withColumn("dt", date_format(from_unixtime(col("timestamp") / 1000), "yyyyMMdd"))

    println("\n" + "=" * 100)
    println("创建按 dt 分区 + user_id 分桶的 Iceberg 表")
    println("=" * 100)

    // ========== 使用 Iceberg API 创建分区+分桶表 ==========

    // 1. 初始化 Hadoop Catalog
    val hadoopConf = new Configuration()
    val warehousePath = "file:///Users/wenzhilong/warehouse/space/iceberg/files/iceberg_warehouse"
    val catalog = new HadoopCatalog(hadoopConf, warehousePath)

    val tableIdentifier = TableIdentifier.of("default", "user_behavior_part_bucket")
    val fullTableName = "iceberg_local.default.user_behavior_part_bucket"

    // 2. 表不存在
    if (!catalog.tableExists(tableIdentifier)) {
      // 3. 定义 Iceberg Schema（从 Spark DataFrame Schema 转换）
      val sparkSchema = df.schema
      val icebergSchema = SparkSchemaUtil.convert(sparkSchema)

      println("\n转换后的 Iceberg Schema:")
      println(icebergSchema)

      // 4. 定义分区规范：先按 dt 分区，再按 user_id 分 5 桶
      val partitionSpec = PartitionSpec
        .builderFor(icebergSchema)
        .identity("dt") // 1. 按 dt 字段分区（身份转换）
        .bucket("user_id", 5) // 2. 在每个分区内按 user_id 字段分 5 桶
        .build()

      println("\n分区规范:")
      println(partitionSpec)
      println(s"  - 第一级分区: dt (identity)")
      println(s"  - 第二级分桶: user_id (bucket[5])")

      // 5. 定义排序顺序：按 user_id 和 timestamp 排序（桶内排序）
      val sortOrder = SortOrder
        .builderFor(icebergSchema)
        .asc("user_id") // 先按 user_id 升序
        .asc("timestamp") // 再按 timestamp 升序
        .build()

      println("\n排序规范:")
      println(sortOrder)
      println(s"  - 排序字段: user_id ASC, timestamp ASC")
      println(s"  - 作用范围: 文件内排序（不跨文件）")

      // 6. 使用 Catalog 创建表（带分区、分桶和排序）
      val table = catalog.buildTable(tableIdentifier, icebergSchema)
        .withPartitionSpec(partitionSpec)
        .withSortOrder(sortOrder)
        .create()

      println(s"\n✓ 表创建成功: ${table.location()}")
      println(s"  - Schema: ${icebergSchema.columns().size()} 列")
      println(s"  - 分区字段: ${partitionSpec.fields().asScala.map(_.name()).mkString(", ")}")
      println(s"  - 排序字段: ${sortOrder.fields().asScala.map(f => s"${icebergSchema.findColumnName(f.sourceId())} ${f.direction()}").mkString(", ")}")

      // 7. 使用 DataFrame API 写入数据
      println("\n开始写入数据...")
      df.writeTo(fullTableName)
        .append()

      println("✓ 数据写入完成！")
    } else {
      // ========== 表已存在：直接插入数据 ==========
      println(s"\n✓ 表 $fullTableName 已存在")

      // 加载表查看元数据
      val table = catalog.loadTable(tableIdentifier)
      println(s"  - 表位置: ${table.location()}")
      println(s"  - 分区规范: ${table.spec()}")
      println(s"  - 排序顺序: ${table.sortOrder()}")

      // 查询当前数据量
      val beforeCount = spark.sql(s"SELECT COUNT(*) as count FROM $fullTableName")
        .collect()(0).getLong(0)
      println(s"  - 插入前数据量: $beforeCount 条")

      // 追加数据
      println("\n开始追加数据...")
      df.writeTo(fullTableName)
        .append()

      // 查询插入后数据量
      val afterCount = spark.sql(s"SELECT COUNT(*) as count FROM $fullTableName")
        .collect()(0).getLong(0)
      println(s"✓ 数据追加完成！")
      println(s"  - 插入后数据量: $afterCount 条")
      println(s"  - 新增数据量: ${afterCount - beforeCount} 条")
    }


    // 8. 验证表信息
    println("\n" + "=" * 100)
    println("验证表信息")
    println("=" * 100)

    // 查询表的分区和排序信息
    println("\n【分区和排序规范】")
    spark.sql("DESCRIBE EXTENDED iceberg_local.default.user_behavior_part_bucket")
      .filter(col("col_name").contains("Part") ||
        col("col_name").contains("Sort") ||
        col("col_name") === "# Partitioning" ||
        col("col_name") === "# Sort Order")
      .show(false)

    // 查询数据总数
    val totalCount = spark.sql("SELECT COUNT(*) as count FROM iceberg_local.default.user_behavior_part_bucket")
      .collect()(0).getLong(0)
    println(s"\n【数据总数】$totalCount 条")

    // 查询数据分布（按 dt 和 user_id 桶）
    println("\n【数据分布】按 dt 分组:")
    spark.sql(
      """
        |SELECT dt, COUNT(*) as count
        |FROM iceberg_local.default.user_behavior_part_bucket
        |GROUP BY dt
        |ORDER BY dt
        |""".stripMargin
    ).show()

    // 查询数据验证排序（从某个桶中取前10条记录）
    println("\n【验证排序】查询某个桶的数据（应该按 user_id, timestamp 排序）:")
    spark.sql(
      """
        |SELECT user_id, timestamp, behavior
        |FROM iceberg_local.default.user_behavior_part_bucket
        |ORDER BY user_id, timestamp
        |LIMIT 10
        |""".stripMargin
    ).show()

    // 查询文件信息
    println("\n【文件信息】")
    val files = spark.sql("SELECT file_path, record_count, file_size_in_bytes FROM iceberg_local.default.user_behavior_part_bucket.files")
    println(s"数据文件数: ${files.count()}")
    files.show(false)

    // 验证分区和分桶结构
    println("\n【分区目录结构】")
    println("文件路径展示了分区和分桶结构：")
    files.select("file_path")
      .collect()
      .foreach { row =>
        val path = row.getString(0)
        // 提取路径中的分区和分桶信息
        if (path.contains("dt=") && path.contains("user_id_bucket=")) {
          val dtPart = path.split("dt=")(1).split("/")(0)
          val bucketPart = path.split("user_id_bucket=")(1).split("/")(0)
          println(s"  dt=$dtPart, user_id_bucket=$bucketPart")
        }
      }

    spark.stop()
  }
}
