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

import org.apache.spark.sql.SparkSession

/**
 * Main entry point for Iceberg Spark demos.
 *
 * Usage:
 * - Run without arguments to see available demos
 * - Run with "rdd" to execute RDD API demo
 * - Run with "sql" to execute SQL demo
 */
object Main {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("demo")
      .master("local[*]")
      .getOrCreate()

    spark.sql(
        """
          |select 1 as id, 'a' as data
          |union all
          |select 2 as id, 'b' as data
          |union all
          |select 3 as id, 'c' as data
          |""".stripMargin
      )
      .show()

    spark.stop()
  }
}
