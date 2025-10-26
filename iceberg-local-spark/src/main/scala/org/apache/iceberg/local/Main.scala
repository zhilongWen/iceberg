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

/**
 * Main entry point for Iceberg Spark demos.
 *
 * Usage:
 *   ./gradlew :iceberg-local-spark:run --args="<demo_name>"
 *
 * Available demos:
 *   - cow-test   : Copy-on-Write 模式功能验证（验证为什么没有 delete 文件）
 *   - mor-test   : Merge-on-Read 模式功能验证（验证 delete 文件生成）
 *   - rdd        : Spark RDD API 演示
 *   - sql        : Spark SQL 演示
 *   - partition  : 分区表主键演示
 */
object Main {
  def main(args: Array[String]): Unit = {
    if (args.isEmpty) {
      printUsage()
      return
    }

    val demo = args(0).toLowerCase

    demo match {
      case "cow-test" =>
        println("启动 Copy-on-Write 模式功能验证测试...")
        SparkIcebergCowSuite.main(args)

      case "mor-test" =>
        println("启动 Merge-on-Read 模式功能验证测试...")
        SparkIcebergMorSuite.main(args)

      case "rdd" =>
        println("启动 RDD API 演示...")
        SparkRddDemo.main(args)

      case "sql" =>
        println("启动 SQL 演示...")
        SparkSqlDemo.main(args)

      case "partition" =>
        println("启动分区表主键演示...")
        SparkWriteWithPartitionPrimaryKeySuite.main(args)

      case _ =>
        println(s"❌ 未知的 demo: $demo\n")
        printUsage()
        System.exit(1)
    }
  }

  private def printUsage(): Unit = {
    println(
      """
        |╔════════════════════════════════════════════════════════════════╗
        |║          Iceberg Spark Demos - Usage Guide                    ║
        |╚════════════════════════════════════════════════════════════════╝
        |
        |使用方法:
        |  ./gradlew :iceberg-local-spark:run --args="<demo_name>"
        |
        |可用的 Demo:
        |
        |  🔬 cow-test   Copy-on-Write 功能验证测试
        |                - 验证为什么 SparkWriteWithPartitionPrimaryKeySuite
        |                  没有生成 delete 文件
        |                - 显示详细的原因分析
        |                - 表名: iceberg_test.default.user_behavior_cow_test
        |
        |  🔬 mor-test   Merge-on-Read 功能验证测试
        |                - 验证 MoR 模式会生成 delete 文件
        |                - 对比 CoW 和 MoR 的区别
        |                - 表名: iceberg_test.default.user_behavior_mor_test
        |
        |  📊 rdd        Spark RDD API 演示
        |                - 使用 RDD 操作读写 Iceberg 表
        |
        |  📝 sql        Spark SQL 演示
        |                - DDL、DML、查询、JOIN、聚合
        |
        |  🔑 partition  分区表主键演示
        |                - MERGE INTO 实现 UPSERT
        |                - 默认使用 CoW 模式（不生成 delete 文件）
        |
        |示例:
        |  # 验证为什么没有 delete 文件（推荐先运行）
        |  ./gradlew :iceberg-local-spark:run --args="cow-test"
        |
        |  # 验证 MoR 模式生成 delete 文件
        |  ./gradlew :iceberg-local-spark:run --args="mor-test"
        |
        |提示:
        |  - 推荐按顺序运行 cow-test 和 mor-test 理解原理
        |  - 运行后可查看 /tmp/iceberg-test-* 和 /tmp/iceberg-warehouse-*
        |    目录的文件结构
        |
        |""".stripMargin)
  }
}
