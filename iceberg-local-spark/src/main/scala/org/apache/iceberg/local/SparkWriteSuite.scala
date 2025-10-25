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

object SparkWriteSuite {
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

    // 方式1：使用 Hadoop Catalog 检查表是否存在
    val hadoopConf = new Configuration()
    val warehousePath = "file:///Users/wenzhilong/warehouse/space/iceberg/files/iceberg_warehouse"
    val catalog = new HadoopCatalog(hadoopConf, warehousePath)

    // 注意：表名必须与 writeTo 中的表名一致
    val tableIdentifier = TableIdentifier.of("default", "user_behavior")
    val fullTableName = "iceberg_local.default.user_behavior"

    if (catalog.tableExists(tableIdentifier)) {
      println(s"表 $fullTableName 已存在，追加数据...")
      sourceData.writeTo(fullTableName)
        .append()
    } else {
      println(s"表 $fullTableName 不存在，创建并写入数据...")
      sourceData.writeTo(fullTableName)
        .create()
    }

    println("数据写入完成！")

    // 方式2：使用 Spark Catalog 检查（更简单）
    /*
    val fullTableName = "iceberg_local.default.user_behavior"
    if (spark.catalog.tableExists("iceberg_local", "default", "user_behavior")) {
      println(s"表 $fullTableName 已存在，追加数据...")
      sourceData.writeTo(fullTableName).append()
    } else {
      println(s"表 $fullTableName 不存在，创建并写入数据...")
      sourceData.writeTo(fullTableName).create()
    }
    */

    // 方式3：使用 CREATE OR REPLACE（最简单）
    /*
    sourceData.writeTo("iceberg_local.default.user_behavior")
      .createOrReplace()  // 如果存在就替换，不存在就创建
    */

    spark.stop()
  }
}
