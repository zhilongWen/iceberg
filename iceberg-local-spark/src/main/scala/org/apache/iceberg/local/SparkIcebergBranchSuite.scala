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
import org.apache.spark.sql.functions._

import scala.collection.JavaConverters._

/**
 * Iceberg 分支（Branch）功能演示
 *
 * 基于表：iceberg_local.default.user_behavior_branch
 * Warehouse：file:///Users/wenzhilong/warehouse/space/iceberg/files/iceberg_warehouse
 *
 * 演示流程：
 * 1. 创建表并在主分支写入初始数据
 * 2. 创建 dev 分支
 * 3. 在 dev 分支写入新数据
 * 4. 分别查询主分支和 dev 分支的数据
 * 5. 将 dev 分支数据合并到主分支
 * 6. 查询主分支所有数据
 *
 * 适用场景：
 * - 测试环境：在分支上测试新功能，不影响生产数据
 * - 并行开发：多个团队在不同分支上开发
 * - 数据验证：在分支上验证数据后再合并到主分支
 */
object SparkIcebergBranchSuite {

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
    println("🌿 Iceberg 分支（Branch）功能演示")
    println("=" * 100)

    val warehousePath = "file:///Users/wenzhilong/warehouse/space/iceberg/files/iceberg_warehouse"
    val catalog = new HadoopCatalog(new Configuration(), warehousePath)
    val tableIdentifier = TableIdentifier.of("default", "user_behavior_branch")
    val fullTableName = "iceberg_local.default.user_behavior_branch"

    def builder_behavior(idx: Int): String = {
      idx % 4 match {
        case 0 => "pv"
        case 1 => "buy"
        case 2 => "cart"
        case 3 => "fav"
      }
    }

    // ========== 步骤1：创建表并在主分支写入初始数据 ==========
    println("\n" + "=" * 100)
    println("【步骤1：创建表并在主分支写入初始数据】")
    println("=" * 100)

    if (!catalog.tableExists(tableIdentifier)) {
      println("\n创建表 user_behavior_branch...")

      // 生成初始数据（user_id 1-20）
      val initialData = Range(1, 21).map { idx =>
          UserBehavior(
            user_id = idx,
            item_id = idx * 100 + idx,
            behavior = builder_behavior(idx),
            timestamp = System.currentTimeMillis()
          )
        }.toDF()
        .withColumn("dt", date_format(from_unixtime(col("timestamp") / 1000), "yyyyMMdd"))

      val icebergSchema = SparkSchemaUtil.convert(initialData.schema)

      // 定义分区规范：按 dt 分区
      val partitionSpec = PartitionSpec
        .builderFor(icebergSchema)
        .identity("dt")
        .build()

      // 定义排序顺序
      val sortOrder = SortOrder
        .builderFor(icebergSchema)
        .asc("user_id")
        .asc("timestamp")
        .build()

      catalog.buildTable(tableIdentifier, icebergSchema)
        .withPartitionSpec(partitionSpec)
        .withSortOrder(sortOrder)
        .withProperty("format-version", "2")
        .create()

      println(s"✓ 表创建成功: $fullTableName")

      // 写入初始数据到主分支
      initialData.writeTo(fullTableName).append()

      val count = spark.sql(s"SELECT COUNT(*) as cnt FROM $fullTableName").collect()(0).getLong(0)
      println(s"✓ 主分支初始数据写入完成: $count 条 (user_id 1-20)")
    }
    else {
      println(s"\n✓ 表已存在: $fullTableName")

      // 如果表已存在，清空数据重新开始演示
      println("清空现有数据以重新演示...")
      spark.sql(s"DELETE FROM $fullTableName WHERE true")

      // 删除所有分支
      val table = catalog.loadTable(tableIdentifier)
      val existingBranches = table.refs().asScala.filter(_._2.isBranch)
      existingBranches.keys.foreach { branchName =>
        try {
          spark.sql(s"ALTER TABLE $fullTableName DROP BRANCH IF EXISTS $branchName")
          println(s"✓ 删除旧分支: $branchName")
        } catch {
          case e: Exception => println(s"⚠️  删除分支失败: ${e.getMessage}")
        }
      }

      // 写入初始数据
      val initialData = Range(1, 21).map { idx =>
          UserBehavior(
            user_id = idx,
            item_id = idx * 100 + idx,
            behavior = builder_behavior(idx),
            timestamp = System.currentTimeMillis()
          )
        }.toDF()
        .withColumn("dt", date_format(from_unixtime(col("timestamp") / 1000), "yyyyMMdd"))

      initialData.writeTo(fullTableName).append()

      val count = spark.sql(s"SELECT COUNT(*) as cnt FROM $fullTableName").collect()(0).getLong(0)
      println(s"✓ 主分支数据重置完成: $count 条 (user_id 1-20)")
    }

    println("\n【主分支 Schema】")
    spark.sql(s"DESCRIBE $fullTableName").show(false)
    spark.sql(s"select * from $fullTableName").show(false)

    // ========== 步骤2：创建 dev 分支 ==========
    println("\n" + "=" * 100)
    println("【步骤2：创建 dev 分支】")
    println("=" * 100)

    val branchName = "dev_branch"
    val branchTableName = s"$fullTableName.branch_$branchName"

    println(s"\n基于主分支当前快照创建分支: $branchName")
    spark.sql(s"ALTER TABLE $fullTableName CREATE BRANCH IF NOT EXISTS $branchName")
    println(s"✓ 分支创建成功: $branchName")

    // 验证分支
    val table = catalog.loadTable(tableIdentifier)
    table.refresh()
    val branches = table.refs().asScala.filter(_._2.isBranch)
    println(s"\n当前分支列表: ${branches.keys.mkString(", ")}")

    // ========== 步骤3：在 dev 分支写入新数据 ==========
    println("\n" + "=" * 100)
    println("【步骤3：在 dev 分支写入新数据】")
    println("=" * 100)

    // 生成 dev 分支的新数据（user_id 21-40）
    val devBranchData = Range(21, 41).map { idx =>
        UserBehavior(
          user_id = idx,
          item_id = idx * 100 + idx,
          behavior = builder_behavior(idx),
          timestamp = System.currentTimeMillis()
        )
      }.toDF()
      .withColumn("dt", date_format(from_unixtime(col("timestamp") / 1000), "yyyyMMdd"))

    println(s"\n准备写入 ${devBranchData.count()} 条数据到 dev 分支 (user_id 21-40)...")
    devBranchData.writeTo(branchTableName).append()

    val devCount = spark.sql(s"SELECT COUNT(*) as cnt FROM $branchTableName").collect()(0).getLong(0)
    println(s"✓ dev 分支数据写入完成，当前记录数: $devCount 条")

    // ========== 步骤4：分别查询主分支和 dev 分支的数据 ==========
    println("\n" + "=" * 100)
    println("【步骤4：分别查询主分支和 dev 分支的数据】")
    println("=" * 100)

    val mainCount = spark.sql(s"SELECT COUNT(*) as cnt FROM $fullTableName").collect()(0).getLong(0)
    println(s"\n【主分支数据】记录数: $mainCount 条")
    println("主分支数据预览 (前10条):")
    spark.sql(s"SELECT user_id, item_id, behavior, dt FROM $fullTableName ORDER BY user_id LIMIT 10").show(false)

    println(s"\n【dev 分支数据】记录数: $devCount 条")
    println("dev 分支数据预览 (前10条):")
    spark.sql(s"SELECT user_id, item_id, behavior, dt FROM $branchTableName ORDER BY user_id LIMIT 10").show(false)

    println(s"\n📊 数据对比:")
    println(s"  - 主分支: $mainCount 条 (user_id 1-20)")
    println(s"  - dev 分支: $devCount 条 (user_id 1-20 + 21-40)")
    println(s"  - dev 分支比主分支多: ${devCount - mainCount} 条")

    // ========== 步骤5：将 dev 分支数据合并到主分支 ==========
    println("\n" + "=" * 100)
    println("【步骤5：将 dev 分支数据合并到主分支】")
    println("=" * 100)

    println("\n从 dev 分支读取新增数据 (user_id 21-40)...")
    val newRecordsFromDev = spark.sql(
      s"""
         |SELECT user_id, item_id, behavior, timestamp, dt
         |FROM $branchTableName
         |WHERE user_id >= 21 AND user_id <= 40
         |""".stripMargin
    )

    val newRecordsCount = newRecordsFromDev.count()
    println(s"✓ 读取到 $newRecordsCount 条新记录")

    println("\n使用 MERGE INTO 将数据合并到主分支...")
    newRecordsFromDev.createOrReplaceTempView("dev_new_records")

    spark.sql(
      s"""
         |MERGE INTO $fullTableName AS target
         |USING dev_new_records AS source
         |ON target.dt = source.dt AND target.user_id = source.user_id
         |WHEN MATCHED THEN UPDATE SET *
         |WHEN NOT MATCHED THEN INSERT *
         |""".stripMargin
    )

    println("✓ 数据合并完成")

    // ========== 步骤6：查询主分支所有数据 ==========
    println("\n" + "=" * 100)
    println("【步骤6：查询主分支所有数据（合并后）】")
    println("=" * 100)

    val finalMainCount = spark.sql(s"SELECT COUNT(*) as cnt FROM $fullTableName").collect()(0).getLong(0)
    println(s"\n【主分支最终数据】记录数: $finalMainCount 条")

    println("\n主分支所有数据:")
    spark.sql(s"SELECT user_id, item_id, behavior, dt FROM $fullTableName ORDER BY user_id").show(50, false)

    println(s"\n✅ 合并验证:")
    println(s"  - 合并前主分支: $mainCount 条")
    println(s"  - 从 dev 分支合并: $newRecordsCount 条")
    println(s"  - 合并后主分支: $finalMainCount 条")
    println(s"  - 新增记录: ${finalMainCount - mainCount} 条")

    if (finalMainCount == mainCount + newRecordsCount) {
      println("\n🎉 数据合并成功！所有 dev 分支数据已成功合并到主分支")
    } else {
      println("\n⚠️  数据合并可能存在问题，请检查")
    }

    // ========== 步骤7：查看分支快照历史 ==========
    println("\n" + "=" * 100)
    println("【步骤7：查看分支快照历史】")
    println("=" * 100)

    println("\n【主分支快照历史】")
    spark.sql(s"SELECT snapshot_id, committed_at, operation FROM $fullTableName.snapshots ORDER BY committed_at").show(false)

    println("\n【dev 分支快照历史】")
    println("使用 Java API 查看分支快照...")
    table.refresh()
    val devBranchRef = table.refs().get(branchName)
    if (devBranchRef != null) {
      val devSnapshotId = devBranchRef.snapshotId()
      println(s"dev 分支当前快照 ID: $devSnapshotId")

      // 遍历分支的快照历史
      var currentSnapshot = table.snapshot(devSnapshotId)
      var snapshotCount = 0
      println("\ndev 分支快照历史:")
      while (currentSnapshot != null && snapshotCount < 10) {
        val timestamp = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date(currentSnapshot.timestampMillis()))
        println(s"  Snapshot ${currentSnapshot.snapshotId()} at $timestamp - ${currentSnapshot.operation()}")
        currentSnapshot = if (currentSnapshot.parentId() != null) table.snapshot(currentSnapshot.parentId()) else null
        snapshotCount += 1
      }
    } else {
      println(s"⚠️  未找到分支: $branchName")
    }

    // ========== 总结 ==========
    println("\n" + "=" * 100)
    println("🎉 分支演示完成！")
    println("=" * 100)

    println(
      s"""
         |
         |📝 演示总结：
         |
         |✅ 【完成的操作】
         |1. 创建表并在主分支写入初始数据 (user_id 1-20)
         |2. 基于主分支创建 dev 分支
         |3. 在 dev 分支写入新数据 (user_id 21-40)
         |4. 分别查询主分支和 dev 分支数据
         |5. 将 dev 分支数据合并到主分支 (使用 MERGE INTO)
         |6. 查询主分支所有数据验证合并结果
         |
         |📊 【最终结果】
         |- 主分支最终包含: $finalMainCount 条记录 (user_id 1-40)
         |- dev 分支保持: $devCount 条记录 (user_id 1-40)
         |- 分支之间完全独立，互不影响
         |
         |💡 【关键概念】
         |1. 分支隔离：dev 分支和主分支的数据完全独立
         |2. 独立快照：每个分支有自己的快照历史
         |3. 灵活合并：可以选择性地合并分支数据到主分支
         |4. Schema 一致：分支继承主分支的 Schema 结构
         |
         |🔧 【实际应用场景】
         |1. 功能测试：在 dev 分支测试新功能，验证通过后合并到主分支
         |2. 数据验证：在分支上处理数据，验证正确性后再合并
         |3. 并行开发：多个团队在不同分支上开发，互不干扰
         |4. 数据备份：保留分支作为历史版本，需要时可以回滚
         |
         |⚠️  【注意事项】
         |1. 使用 ALTER TABLE CREATE BRANCH 创建分支
         |2. 使用 table.branch_xxx 语法访问分支数据
         |3. 分支继承主分支的 Schema 变更
         |4. 数据合并需要手动操作（MERGE INTO 或 INSERT）
         |5. 分支会占用元数据空间，不使用时建议删除
         |
         |📂 【后续操作】
         |1. 查看分支列表：
         |   spark.sql("SHOW BRANCHES IN $fullTableName")
         |
         |2. 查询分支数据：
         |   SELECT * FROM $branchTableName
         |
         |3. Time Travel 到历史快照：
         |   SELECT * FROM $branchTableName VERSION AS OF snapshot_id
         |
         |4. 删除分支：
         |   spark.sql("ALTER TABLE $fullTableName DROP BRANCH IF EXISTS $branchName")
         |
         |5. 查看快照历史：
         |   SELECT * FROM $fullTableName.snapshots
         |""".stripMargin)

    spark.stop()
  }
}
