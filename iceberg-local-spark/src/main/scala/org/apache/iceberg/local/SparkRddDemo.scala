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
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.hadoop.HadoopCatalog
import org.apache.iceberg.{PartitionSpec, Schema}
import org.apache.iceberg.spark.SparkSchemaUtil
import org.apache.iceberg.types.Types
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._

/**
 * Spark RDD API demo for Iceberg.
 *
 * This example demonstrates:
 * 1. Creating an Iceberg table with Hadoop catalog
 * 2. Using Spark RDD to transform data
 * 3. Writing RDD data to Iceberg table
 * 4. Reading data from Iceberg table using RDD operations
 */
object SparkRddDemo {

  def main(args: Array[String]): Unit = {
    println("=" * 60)
    println("Spark RDD API Demo for Apache Iceberg")
    println("=" * 60)
    println()

    // Create Spark session
    val spark = SparkSession.builder()
      .appName("Iceberg Spark RDD Demo")
      .master("local[*]")
      .config("spark.sql.catalog.hadoop_catalog", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.hadoop_catalog.type", "hadoop")
      .config("spark.sql.catalog.hadoop_catalog.warehouse", "file:///tmp/iceberg-warehouse-spark-rdd")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try {
      println("Step 1: Create Iceberg Table")
      println("-" * 60)

      // Define warehouse path
      val warehousePath = "file:///tmp/iceberg-warehouse-spark-rdd"

      // Create Hadoop catalog
      val hadoopConf = new Configuration()
      val catalog = new HadoopCatalog(hadoopConf, warehousePath)

      // Define table identifier
      val tableId = TableIdentifier.of("default", "sales_data")

      // Define schema
      val schema = new Schema(
        Types.NestedField.required(1, "sale_id", Types.LongType.get()),
        Types.NestedField.required(2, "product", Types.StringType.get()),
        Types.NestedField.required(3, "quantity", Types.IntegerType.get()),
        Types.NestedField.required(4, "price", Types.DoubleType.get()),
        Types.NestedField.required(5, "region", Types.StringType.get())
      )

      // Create or load table
      val table = if (!catalog.tableExists(tableId)) {
        val spec = PartitionSpec.builderFor(schema)
          .identity("region")
          .build()
        catalog.createTable(tableId, schema, spec)
      } else {
        catalog.loadTable(tableId)
      }

      println(s"✓ Table created/loaded: ${table.location()}")
      println()

      println("Step 2: Create and Transform Data using RDD")
      println("-" * 60)

      // Create sample sales data
      val salesData = Seq(
        (1L, "Laptop", 2, 1200.0, "North"),
        (2L, "Mouse", 10, 25.0, "South"),
        (3L, "Keyboard", 5, 75.0, "North"),
        (4L, "Monitor", 3, 300.0, "East"),
        (5L, "Laptop", 1, 1200.0, "West"),
        (6L, "Mouse", 15, 25.0, "North"),
        (7L, "Desk", 2, 350.0, "South"),
        (8L, "Chair", 4, 150.0, "East"),
        (9L, "Monitor", 2, 300.0, "West"),
        (10L, "Keyboard", 3, 75.0, "South")
      )

      // Create RDD from data
      val salesRDD = spark.sparkContext.parallelize(salesData)

      // Transform: Calculate total amount for each sale
      val transformedRDD = salesRDD.map { case (id, product, qty, price, region) =>
        val total = qty * price
        println(s"  Processing: Sale #$id - $product x $qty @ $$$price = $$$total")
        Row(id, product, qty, price, region)
      }

      println(s"✓ Created and transformed ${salesData.size} records using RDD")
      println()

      println("Step 3: Write RDD Data to Iceberg Table")
      println("-" * 60)

      // Define Spark schema
      val sparkSchema = StructType(Seq(
        StructField("sale_id", LongType, nullable = false),
        StructField("product", StringType, nullable = false),
        StructField("quantity", IntegerType, nullable = false),
        StructField("price", DoubleType, nullable = false),
        StructField("region", StringType, nullable = false)
      ))

      // Convert RDD to DataFrame
      val salesDF = spark.createDataFrame(transformedRDD, sparkSchema)

      // Write to Iceberg table
      salesDF.writeTo("hadoop_catalog.default.sales_data")
        .append()

      println("✓ Data written to Iceberg table")
      println()

      println("Step 4: Read and Process Data using RDD Operations")
      println("-" * 60)

      // Read data from Iceberg table
      val readDF = spark.table("hadoop_catalog.default.sales_data")

      // Convert to RDD for RDD operations
      val dataRDD = readDF.rdd

      println("\n--- RDD Operation 1: Filter products by region (North) ---")
      val northSales = dataRDD
        .filter(row => row.getAs[String]("region") == "North")
        .collect()

      northSales.foreach { row =>
        println(s"  ${row.getAs[String]("product")} - Qty: ${row.getAs[Int]("quantity")} - Region: ${row.getAs[String]("region")}")
      }

      println("\n--- RDD Operation 2: Calculate total revenue by product ---")
      val revenueByProduct = dataRDD
        .map { row =>
          val product = row.getAs[String]("product")
          val quantity = row.getAs[Int]("quantity")
          val price = row.getAs[Double]("price")
          (product, quantity * price)
        }
        .reduceByKey(_ + _)
        .collect()
        .sortBy(-_._2)

      revenueByProduct.foreach { case (product, revenue) =>
        println(f"  $product%-15s: $$${revenue}%.2f")
      }

      println("\n--- RDD Operation 3: Calculate total quantity by region ---")
      val quantityByRegion = dataRDD
        .map { row =>
          (row.getAs[String]("region"), row.getAs[Int]("quantity"))
        }
        .reduceByKey(_ + _)
        .collect()
        .sortBy(-_._2)

      quantityByRegion.foreach { case (region, total) =>
        println(f"  $region%-15s: $total units")
      }

      println("\n--- RDD Operation 4: Find products with quantity > 5 ---")
      val highQuantityProducts = dataRDD
        .filter(row => row.getAs[Int]("quantity") > 5)
        .map { row =>
          (row.getAs[String]("product"), row.getAs[Int]("quantity"))
        }
        .collect()

      highQuantityProducts.foreach { case (product, qty) =>
        println(s"  $product - Quantity: $qty")
      }

      println("\n--- RDD Operation 5: Count sales by product (using mapPartitions) ---")
      val countByProduct = dataRDD
        .mapPartitions { partition =>
          val counts = scala.collection.mutable.Map[String, Int]()
          partition.foreach { row =>
            val product = row.getAs[String]("product")
            counts(product) = counts.getOrElse(product, 0) + 1
          }
          counts.iterator
        }
        .reduceByKey(_ + _)
        .collect()
        .sortBy(-_._2)

      countByProduct.foreach { case (product, count) =>
        println(f"  $product%-15s: $count sales")
      }

      println()
      println("=" * 60)
      println("Demo completed successfully!")
      println("=" * 60)
      println()
      println("Data location: /tmp/iceberg-warehouse-spark-rdd/default/sales_data")
      println("You can verify the data by:")
      println("  1. Checking the warehouse directory")
      println("  2. Using Spark SQL to query the table")
      println("  3. Reading with other engines (Flink, Trino, etc.)")

    } finally {
      spark.stop()
    }
  }
}
