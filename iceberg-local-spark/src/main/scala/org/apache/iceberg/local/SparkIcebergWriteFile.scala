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
import org.apache.iceberg.{PartitionSpec, Schema}
import org.apache.iceberg.catalog.{Namespace, TableIdentifier}
import org.apache.iceberg.hadoop.HadoopCatalog
import org.apache.iceberg.spark.SparkSchemaUtil
import org.apache.iceberg.types.Types
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, date_format, from_unixtime}
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._

case class UserBehavior(user_id: Int, item_id: Long, behavior: String, timestamp: Long)

object SparkIcebergWriteFile {
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

    // 使用 from_unixtime 将毫秒时间戳转换为日期格式
    // timestamp 是毫秒，需要除以 1000 转换为秒
    val df = sourceData.withColumn("dt", date_format(from_unixtime(col("timestamp") / 1000), "yyyyMMdd"))

    df.show()

    println("\n" + "=" * 80)
    println("使用 Iceberg API + DataFrame API 创建带分桶的表")
    println("=" * 80)

    // ========== 使用 Iceberg API 创建表结构，然后用 DataFrame 写入 ==========

    // 1. 初始化 Hadoop Catalog
    val hadoopConf = new Configuration()
    val warehousePath = "file:///Users/wenzhilong/warehouse/space/iceberg/files/iceberg_warehouse"
    val catalog = new HadoopCatalog(hadoopConf, warehousePath)

    val tableIdentifier = TableIdentifier.of("default", "user_behavior_local")

    // 2. 删除旧表（如果存在）
    if (catalog.tableExists(tableIdentifier)) {
      catalog.dropTable(tableIdentifier)
      println(s"已删除旧表: $tableIdentifier")
    }

    // 3. 定义 Iceberg Schema（从 Spark DataFrame Schema 转换）
    val sparkSchema = df.schema
    val icebergSchema = SparkSchemaUtil.convert(sparkSchema)

    println("\n转换后的 Iceberg Schema:")
    println(icebergSchema)

    // 4. 定义分区规范（包括分桶）
    val partitionSpec = PartitionSpec
      .builderFor(icebergSchema)
      .identity("dt") // 按 dt 字段分区（身份转换）
      .bucket("behavior", 4) // 按 behavior 字段分桶，4个桶
      .build()

    println("\n分区规范:")
    println(partitionSpec)

    // 5. 使用 Catalog 创建表
    val table = catalog.createTable(
      tableIdentifier,
      icebergSchema,
      partitionSpec
    )

    println(s"\n✓ 表创建成功: ${table.location()}")
    println(s"  - Schema: ${icebergSchema.columns().size()} 列")
    println(s"  - 分区字段: ${partitionSpec.fields().asScala.map(_.name()).mkString(", ")}")

    // 6. 使用 DataFrame API 写入数据
    println("\n开始写入数据...")
    df.writeTo("iceberg_local.default.user_behavior_local")
      .append()

    println("✓ 数据写入完成！")

    // 7. 验证表信息
    println("\n" + "=" * 80)
    println("验证表信息")
    println("=" * 80)

    // 查询表的分区信息
    println("\n【分区规范】")
    spark.sql("DESCRIBE EXTENDED iceberg_local.default.user_behavior_local")
      .filter(col("col_name").contains("Part") || col("col_name") === "# Partitioning")
      .show(false)

    // 查询数据总数
    val totalCount = spark.sql("SELECT COUNT(*) as count FROM iceberg_local.default.user_behavior_local")
      .collect()(0).getLong(0)
    println(s"\n【数据总数】$totalCount 条")

    // 查询数据分布
    println("\n【数据分布】按 dt 和 behavior 分组:")
    spark.sql(
      """
        |SELECT dt, behavior, COUNT(*) as count
        |FROM iceberg_local.default.user_behavior_local
        |GROUP BY dt, behavior
        |ORDER BY dt, behavior
        |""".stripMargin
    ).show()

    // 查询文件信息
    println("\n【文件信息】")
    val files = spark.sql("SELECT file_path, record_count, file_size_in_bytes FROM iceberg_local.default.user_behavior_local.files")
    println(s"数据文件数: ${files.count()}")
    files.show(false)

    spark.stop()
  }
}
