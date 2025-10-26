/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.local

import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.{PartitionSpec, SortOrder}
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.hadoop.HadoopCatalog
import org.apache.iceberg.spark.SparkSchemaUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{date_format, from_unixtime, lit}

import java.io.File
import scala.sys.process._

/**
 * Copy-on-Write (CoW) 模式功能验证测试
 *
 * 目的：验证为什么 SparkWriteWithPartitionPrimaryKeySuite 没有生成 delete 文件
 *
 * 答案：因为 Iceberg 默认使用 Copy-on-Write 模式
 */
object SparkIcebergCowSuite {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName(this.getClass.getSimpleName)
      .config("spark.sql.catalog.iceberg_local", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.iceberg_local.type", "hadoop")
      .config("spark.sql.catalog.iceberg_local.warehouse", "file:///Users/wenzhilong/warehouse/space/iceberg/files/iceberg_warehouse")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .getOrCreate()

    import spark.implicits._

    println("\n" + "=" * 100)
    println("🔵 Copy-on-Write (CoW) 模式功能验证测试")
    println("验证：为什么 SparkWriteWithPartitionPrimaryKeySuite 没有生成 delete 文件")
    println("=" * 100)

    val warehousePath = "file:///Users/wenzhilong/warehouse/space/iceberg/files/iceberg_warehouse"
    val catalog = new HadoopCatalog(new Configuration(), warehousePath)
    val tableIdentifier = TableIdentifier.of("default", "user_behavior_cow")
    val fullTableName = "iceberg_local.default.user_behavior_cow"

    if (!catalog.tableExists(tableIdentifier)) {
      println("\n【步骤1：创建表 - Copy-on-Write 模式】")

      val initData = Seq(UserBehavior(1, 101L, "pv", System.currentTimeMillis())).toDF()
        .withColumn("dt", lit("20241025"))
      val icebergSchema = SparkSchemaUtil.convert(initData.schema)
      val partitionSpec = PartitionSpec.builderFor(icebergSchema).identity("dt").build()
      val sortOrder = SortOrder.builderFor(icebergSchema).asc("user_id").asc("timestamp").build()

      catalog.buildTable(tableIdentifier, icebergSchema)
        .withPartitionSpec(partitionSpec)
        .withSortOrder(sortOrder)
        .withProperty("format-version", "2")
        .withProperty("primary-key", "dt,user_id")
        .withProperty("write.merge.mode", "copy-on-write")
        .withProperty("write.update.mode", "copy-on-write")
        .withProperty("write.delete.mode", "copy-on-write")
        .create()

      println(s"✓ 表创建成功: $fullTableName (CoW 模式)")
    }

    val table = catalog.loadTable(tableIdentifier)
    println(s"\n【表属性】")
    println(s"  primary-key: ${table.properties().get("primary-key")}")
    println(s"  write.merge.mode: ${table.properties().getOrDefault("write.merge.mode", "default")}")

    println("\n【步骤2：初始写入 10 条记录】")
    val batch1 = Seq(
      UserBehavior(1, 101L, "pv", 1729756800000L),
      UserBehavior(2, 102L, "buy", 1729756801000L),
      UserBehavior(3, 103L, "cart", 1729756802000L),
      UserBehavior(4, 104L, "fav", 1729756803000L),
      UserBehavior(5, 105L, "pv", 1729756804000L)
    ).toDF().withColumn("dt", lit("20241024"))
      .union(Seq(
        UserBehavior(1, 201L, "buy", 1729843200000L),
        UserBehavior(2, 202L, "pv", 1729843201000L),
        UserBehavior(3, 203L, "cart", 1729843202000L),
        UserBehavior(4, 204L, "pv", 1729843203000L),
        UserBehavior(5, 205L, "fav", 1729843204000L)
      ).toDF().withColumn("dt", lit("20241025")))

    batch1.writeTo(fullTableName).append()
    val count1 = spark.sql(s"SELECT COUNT(*) as cnt FROM $fullTableName").collect()(0).getLong(0)
    println(s"✓ 写入完成，当前记录数: $count1")

    spark.sql(s"select * from ${fullTableName}").show(1000)

    val initialFiles = countAllFiles("file:///Users/wenzhilong/warehouse/space/iceberg/files/iceberg_warehouse/default/user_behavior_cow/data")
    println(s"【初始文件统计】Data: ${initialFiles._1}, Delete: ${initialFiles._2}")

    println("\n【步骤3：MERGE INTO 操作】")
    val batch2 = Seq(
      UserBehavior(3, 9103L, "buy", 1729756902000L),
      UserBehavior(4, 9104L, "pv", 1729756903000L),
      UserBehavior(5, 9105L, "cart", 1729756904000L),
      UserBehavior(6, 106L, "fav", 1729756905000L),
      UserBehavior(7, 107L, "pv", 1729756906000L)
    ).toDF().withColumn("dt", lit("20241024"))
      .union(Seq(
        UserBehavior(4, 9204L, "buy", 1729843303000L),
        UserBehavior(5, 9205L, "cart", 1729843304000L),
        UserBehavior(6, 206L, "pv", 1729843305000L),
        UserBehavior(7, 207L, "fav", 1729843306000L),
        UserBehavior(8, 208L, "buy", 1729843307000L)
      ).toDF().withColumn("dt", lit("20241025")))
      .union(Seq(
        UserBehavior(1, 301L, "pv", 1729929600000L),
        UserBehavior(2, 302L, "buy", 1729929601000L),
        UserBehavior(3, 303L, "cart", 1729929602000L)
      ).toDF().withColumn("dt", lit("20241026")))

    batch2.createOrReplaceTempView("source_data")
    val beforeMerge = spark.sql(s"SELECT COUNT(*) as cnt FROM $fullTableName").collect()(0).getLong(0)
    println(s"【MERGE 前】记录数: $beforeMerge")

    spark.sql(
      s"""
         |MERGE INTO $fullTableName AS target
         |USING source_data AS source
         |ON target.dt = source.dt AND target.user_id = source.user_id
         |WHEN MATCHED THEN UPDATE SET *
         |WHEN NOT MATCHED THEN INSERT *
         |""".stripMargin
    )

    val afterMerge = spark.sql(s"SELECT COUNT(*) as cnt FROM $fullTableName").collect()(0).getLong(0)
    println(s"【MERGE 后】记录数: $afterMerge，新增: ${afterMerge - beforeMerge} 条")

    spark.sql(s"select * from ${fullTableName}").show(1000)

    val afterFiles = countAllFiles("file:///Users/wenzhilong/warehouse/space/iceberg/files/iceberg_warehouse/default/user_behavior_cow/data")
    println(s"【MERGE 后文件统计】Data: ${afterFiles._1} (+${afterFiles._1 - initialFiles._1}), Delete: ${afterFiles._2}")

    println("\n【核心验证：为什么没有 Delete 文件？】")
    val deleteFiles = afterFiles._2
    if (deleteFiles == 0) {
      println("\n✅ 验证通过！Copy-on-Write 模式不生成 delete 文件")
      println("\n📝 原因分析:")
      println("  1. Iceberg 默认使用 Copy-on-Write (CoW) 模式")
      println("  2. MERGE INTO 执行 UPDATE 时：")
      println("     - 读取受影响分区的数据文件")
      println("     - 在内存中执行 UPDATE")
      println("     - 重写整个数据文件（包含更新后的数据）")
      println("     - 逻辑删除旧文件（元数据中移除）")
      println("  3. 不会生成 position delete 或 equality delete 文件")
      println("\n💡 这就是 SparkWriteWithPartitionPrimaryKeySuite 没有 delete 文件的原因！")
    } else {
      println("❌ 异常：CoW 模式不应该生成 delete 文件")
    }

    println("\n【Copy-on-Write 模式特点】")
    println("✅ 优点: 读取性能高，数据一致性好")
    println("⚠️  缺点: 写入性能低，写放大严重")
    println("🎯 适用: 读多写少的场景")
    println("📌 对比: 运行 SparkIcebergMorSuite 查看 MoR 模式生成的 delete 文件")

    spark.stop()
  }

  private def countAllFiles(path: String): (Int, Int) = {
    val dir = new File(path)
    if (!dir.exists()) return (0, 0)
    try {
      val data = s"find $path -type f -name '*.parquet' ! -name '*delete*' 2>/dev/null | wc -l".!!.trim.toInt
      val delete = s"find $path -type f -name '*delete*.parquet' 2>/dev/null | wc -l".!!.trim.toInt
      (data, delete)
    } catch {
      case _: Exception => (0, 0)
    }
  }
}
