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
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.hadoop.HadoopCatalog
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, date_format, from_unixtime, to_date}

object SparkWriteWithPartitionSuite {
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


    val df = sourceData.withColumn("dt", date_format(from_unixtime(col("timestamp") / 1000), "yyyyMMdd"))

    val fullTableName = "iceberg_local.default.user_behavior_partition"

    //  | 操作                | 是否需要 partitionedBy | 原因           |
    //  |-------------------|--------------------|--------------|
    //  | create()          | ✓ 需要               | 定义表的分区规范     |
    //  | append()          | ✗ 不需要              | 自动使用表已有的分区规范 |
    //  | createOrReplace() | ✓ 需要               | 重新定义表结构和分区规范 |
    //  | replace()         | ✓ 需要               | 替换表结构和分区规范   |
    if (spark.catalog.tableExists(fullTableName)) {
      println(s"表 $fullTableName 已存在，追加数据...")
      df.writeTo(fullTableName)
        .append()
    } else {
      println(s"表 $fullTableName 不存在，创建并写入数据...")
      df.writeTo(fullTableName)
        .partitionedBy(col("dt")) // 按 dt 分区
        .create()
    }

    println("数据写入完成！")


    // 使用 CREATE OR REPLACE（最简单，但会替换整个表）
    /*
    df.writeTo(fullTableName)
      .partitionedBy(col("dt"))
      .createOrReplace()  // 如果存在就替换，不存在就创建
    */

    spark.stop()
  }
}
