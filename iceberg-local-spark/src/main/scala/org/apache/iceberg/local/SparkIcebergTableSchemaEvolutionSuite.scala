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
 * Iceberg Schema Evolution 功能演示（基于 Main 分支）
 *
 * 表名：iceberg_local.default.user_behavior_schema_evolution
 * Warehouse：file:///Users/wenzhilong/warehouse/space/iceberg/files/iceberg_warehouse
 *
 * 演示内容：
 * 1. 添加新列（Add Column）
 * 2. 删除列（Drop Column）
 * 3. 重命名列（Rename Column）
 * 4. 修改列类型（Alter Column Type）
 * 5. 修改列顺序（Reorder Columns）
 * 6. 添加列注释（Add Column Comment）
 * 7. 设置列为必填（Set Column Required）
 *
 * 重点：所有操作都在 Main 分支上直接进行，证明 Schema Evolution 不需要分支
 */
object SparkIcebergTableSchemaEvolutionSuite {

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
    println("📊 Iceberg Schema Evolution 功能演示（Main 分支）")
    println("=" * 100)

    val warehousePath = "file:///Users/wenzhilong/warehouse/space/iceberg/files/iceberg_warehouse"
    val catalog = new HadoopCatalog(new Configuration(), warehousePath)
    val tableIdentifier = TableIdentifier.of("default", "user_behavior_schema_evolution")
    val fullTableName = "iceberg_local.default.user_behavior_schema_evolution"

    // ========== 步骤1：创建初始表 ==========
    if (!catalog.tableExists(tableIdentifier)) {
      println("\n【步骤1：创建初始表 - 基础 Schema】")

      val initData = Seq(
        UserBehavior(1, 101L, "pv", System.currentTimeMillis()),
        UserBehavior(2, 102L, "buy", System.currentTimeMillis()),
        UserBehavior(3, 103L, "cart", System.currentTimeMillis())
      ).toDF()
        .withColumn("dt", date_format(from_unixtime(col("timestamp") / 1000), "yyyyMMdd"))

      val icebergSchema = SparkSchemaUtil.convert(initData.schema)
      val partitionSpec = PartitionSpec.builderFor(icebergSchema).identity("dt").build()
      val sortOrder = SortOrder.builderFor(icebergSchema).asc("user_id").asc("timestamp").build()

      catalog.buildTable(tableIdentifier, icebergSchema)
        .withPartitionSpec(partitionSpec)
        .withSortOrder(sortOrder)
        .withProperty("format-version", "2")
        .create()

      println(s"✓ 表创建成功: $fullTableName")

      // 写入初始数据
      initData.writeTo(fullTableName).append()
      println(s"✓ 初始数据写入完成: ${initData.count()} 条")

      println("\n【初始 Schema】")
      spark.sql(s"DESCRIBE $fullTableName").show(false)
    } else {
      println(s"\n✓ 表已存在: $fullTableName")
      println("\n【当前 Schema】")
      spark.sql(s"DESCRIBE $fullTableName").show(false)
    }

    val table = catalog.loadTable(tableIdentifier)

    // ========== 步骤2：添加新列 ==========
    println("\n" + "=" * 100)
    println("【步骤2：添加新列 - ADD COLUMN】")
    println("=" * 100)

    println("\n添加列: status STRING, age INT")
    spark.sql(
      s"""
         |ALTER TABLE $fullTableName
         |ADD COLUMNS (
         |  status STRING COMMENT '用户状态',
         |  age INT COMMENT '用户年龄'
         |)
         |""".stripMargin
    )
    println("✓ 列添加成功")

    println("\n【Schema 变更后】")
    spark.sql(s"DESCRIBE $fullTableName").show(false)
    spark.sql(s"SELECT * FROM $fullTableName").show(1000)

    // 写入带有新列的数据
    println("\n写入带有新列的数据...")
    val newData = Seq(
      (4, 104L, "pv", System.currentTimeMillis(), "active", 25),
      (5, 105L, "buy", System.currentTimeMillis(), "active", 30)
    ).toDF("user_id", "item_id", "behavior", "timestamp", "status", "age")
      .withColumn("dt", date_format(from_unixtime(col("timestamp") / 1000), "yyyyMMdd"))

    newData.writeTo(fullTableName).append()
    println("✓ 数据写入完成")

    println("\n【数据预览】旧数据的新列为 NULL，新数据有值")
    spark.sql(s"SELECT * FROM $fullTableName ORDER BY user_id").show(false)

    // ========== 步骤3：重命名列 ==========
    println("\n" + "=" * 100)
    println("【步骤3：重命名列 - RENAME COLUMN】")
    println("=" * 100)

    println("\n重命名: behavior → action")
    spark.sql(
      s"""
         |ALTER TABLE $fullTableName
         |RENAME COLUMN behavior TO action
         |""".stripMargin
    )
    println("✓ 列重命名成功")

    println("\n【Schema 变更后】")
    spark.sql(s"DESCRIBE $fullTableName").show(false)

    println("\n【数据预览】使用新列名查询")
    spark.sql(s"SELECT * FROM $fullTableName ORDER BY user_id").show(false)

    // ========== 步骤4：修改列注释 ==========
    println("\n" + "=" * 100)
    println("【步骤4：修改列注释 - ALTER COLUMN COMMENT】")
    println("=" * 100)

    println("\n修改 user_id 列的注释")
    spark.sql(
      s"""
         |ALTER TABLE $fullTableName
         |ALTER COLUMN user_id COMMENT '用户唯一标识（主键）'
         |""".stripMargin
    )
    println("✓ 注释修改成功")

    println("\n【Schema 变更后】")
    spark.sql(s"DESCRIBE $fullTableName").show(false)

    // ========== 步骤5：删除列 ==========
    println("\n" + "=" * 100)
    println("【步骤5：删除列 - DROP COLUMN】")
    println("=" * 100)

    println("\n删除列: age")
    spark.sql(
      s"""
         |ALTER TABLE $fullTableName
         |DROP COLUMN age
         |""".stripMargin
    )
    println("✓ 列删除成功")

    println("\n【Schema 变更后】")
    spark.sql(s"DESCRIBE $fullTableName").show(false)

    println("\n【数据预览】age 列已不可见")
    spark.sql(s"SELECT * FROM $fullTableName ORDER BY user_id LIMIT 5").show(false)

    // ========== 步骤6：修改列类型 ==========
    println("\n" + "=" * 100)
    println("【步骤6：修改列类型 - ALTER COLUMN TYPE】")
    println("=" * 100)

    println("\n⚠️  注意：Iceberg 支持有限的类型提升（type promotion）")
    println("支持的类型转换:")
    println("  - int → long")
    println("  - float → double")
    println("  - decimal(P,S) → decimal(P',S) where P' > P")
    println("\n不支持的类型转换:")
    println("  - long → int (精度降低)")
    println("  - string → int (不兼容)")

    println("\n演示：将 item_id 从 LONG 提升为 DECIMAL(20,0)")
    try {
      spark.sql(
        s"""
           |ALTER TABLE $fullTableName
           |ALTER COLUMN item_id TYPE DECIMAL(20, 0)
           |""".stripMargin
      )
      println("✓ 类型修改成功")

      println("\n【Schema 变更后】")
      spark.sql(s"DESCRIBE $fullTableName").show(false)
    } catch {
      case e: Exception =>
        println(s"❌ 类型修改失败: ${e.getMessage}")
        println("可能原因：Spark/Iceberg 版本限制或类型不兼容")
    }

    // ========== 步骤7：查看 Schema 演化历史 ==========
    println("\n" + "=" * 100)
    println("【步骤7：查看 Schema 演化历史】")
    println("=" * 100)

    println("\n【历史快照】每次 Schema 变更都会生成新快照")
    spark.sql(s"SELECT snapshot_id, committed_at, operation, summary FROM $fullTableName.snapshots").show(false)

    println("\n【Schema 历史】")
    table.refresh()
    val schemas = table.schemas()
    println(s"总共有 ${schemas.size()} 个 Schema 版本")
    schemas.asScala.foreach { case (schemaId, schema) =>
      println(s"\nSchema ID: $schemaId")
      println(s"字段: ${schema.columns().asScala.map(_.name()).mkString(", ")}")
    }

    // ========== 步骤8：Time Travel 查看不同 Schema 版本的数据 ==========
    println("\n" + "=" * 100)
    println("【步骤8：Time Travel - 查看不同 Schema 版本】")
    println("=" * 100)

    val snapshots = spark.sql(s"SELECT snapshot_id FROM $fullTableName.snapshots ORDER BY committed_at").collect()
    if (snapshots.length >= 2) {
      val firstSnapshotId = snapshots(0).getLong(0)
      val latestSnapshotId = snapshots(snapshots.length - 1).getLong(0)

      println(s"\n【第一个快照】Snapshot ID: $firstSnapshotId (初始 Schema)")
      spark.sql(s"SELECT * FROM $fullTableName VERSION AS OF $firstSnapshotId LIMIT 3").show(false)

      println(s"\n【最新快照】Snapshot ID: $latestSnapshotId (演化后的 Schema)")
      spark.sql(s"SELECT * FROM $fullTableName VERSION AS OF $latestSnapshotId LIMIT 3").show(false)
    }

    // ========== 步骤9：总结 Schema Evolution 特点 ==========
    println("\n" + "=" * 100)
    println("【步骤9：Schema Evolution 特点总结】")
    println("=" * 100)

    println("""
        |✅ Iceberg Schema Evolution 优势：
        |
        |1. 【完全向后兼容】
        |   - 添加列：旧数据自动填充 NULL
        |   - 删除列：旧数据仍可通过 Time Travel 访问
        |   - 重命名列：无需重写数据，仅更新元数据
        |
        |2. 【无需重写数据】
        |   - 所有 Schema 变更都是元数据操作
        |   - 数据文件保持不变
        |   - 不影响现有数据的物理存储
        |
        |3. 【多版本共存】
        |   - 同一张表可以有多个 Schema 版本
        |   - 通过 Time Travel 访问历史 Schema
        |   - 不同应用可以读取不同版本的 Schema
        |
        |4. 【不需要分支】
        |   - Main 分支完全支持 Schema Evolution
        |   - 分支只是提供额外的隔离和安全性
        |   - 选择是否使用分支取决于团队策略
        |
        |5. 【支持的操作】
        |   ✓ ADD COLUMN       - 添加新列
        |   ✓ DROP COLUMN      - 删除列
        |   ✓ RENAME COLUMN    - 重命名列
        |   ✓ ALTER COMMENT    - 修改注释
        |   ✓ ALTER TYPE       - 类型提升（有限支持）
        |   ✗ REORDER COLUMNS  - Spark SQL 不支持（可用 Java API）
        |
        |6. 【类型提升限制】
        |   ✓ int → long
        |   ✓ float → double
        |   ✓ decimal(P,S) → decimal(P',S) where P' > P
        |   ✗ 不支持精度降低或不兼容的类型转换
        |
        |7. 【最佳实践】
        |   - 生产环境：直接在 main 分支操作（简单变更）
        |   - 测试环境：使用分支隔离（复杂变更）
        |   - 添加列：优先使用可空列
        |   - 删除列：确认没有应用依赖该列
        |""".stripMargin)

    // ========== 步骤10：演示完整的 Schema Evolution 流程 ==========
    println("\n" + "=" * 100)
    println("【步骤10：完整流程演示】")
    println("=" * 100)

    println("\n演示：添加 region 列 → 写入数据 → 删除 status 列 → 验证数据")

    // 添加 region 列
    spark.sql(s"ALTER TABLE $fullTableName ADD COLUMN region STRING COMMENT '用户地区'")
    println("✓ 1. 添加 region 列")

    // 写入新数据
    val finalData = Seq(
      (6, 106L, "buy", System.currentTimeMillis(), "active", "北京"),
      (7, 107L, "cart", System.currentTimeMillis(), "active", "上海")
    ).toDF("user_id", "item_id", "action", "timestamp", "status", "region")
      .withColumn("dt", date_format(from_unixtime(col("timestamp") / 1000), "yyyyMMdd"))

    finalData.writeTo(fullTableName).append()
    println("✓ 2. 写入包含 region 的数据")

    // 删除 status 列
    spark.sql(s"ALTER TABLE $fullTableName DROP COLUMN status")
    println("✓ 3. 删除 status 列")

    println("\n【最终 Schema】")
    spark.sql(s"DESCRIBE $fullTableName").show(false)

    println("\n【最终数据】")
    spark.sql(s"SELECT user_id, action, region, dt FROM $fullTableName ORDER BY user_id").show(false)

    val finalCount = spark.sql(s"SELECT COUNT(*) as cnt FROM $fullTableName").collect()(0).getLong(0)
    println(s"\n✓ 最终记录数: $finalCount 条")

    println("\n" + "=" * 100)
    println("🎉 Schema Evolution 演示完成！")
    println("=" * 100)

    println(s"""
        |
        |📝 关键结论：
        |
        |1. Schema Evolution 可以在 Main 分支直接进行
        |2. 不需要创建分支，除非需要隔离测试
        |3. 所有变更都是元数据操作，不重写数据
        |4. 完全向后兼容，支持 Time Travel
        |5. 适合快速迭代和灵活的数据建模
        |
        |📌 后续操作建议：
        |
        |1. 查看 Schema 历史：
        |   SELECT * FROM $fullTableName.schemas
        |
        |2. Time Travel 到特定 Schema：
        |   SELECT * FROM $fullTableName VERSION AS OF <snapshot_id>
        |
        |3. 回滚到历史快照：
        |   CALL iceberg_local.system.rollback_to_snapshot('$fullTableName', <snapshot_id>)
        |
        |4. 查看表的所有属性：
        |   SHOW TBLPROPERTIES $fullTableName
        |""".stripMargin)

    spark.stop()
  }
}
