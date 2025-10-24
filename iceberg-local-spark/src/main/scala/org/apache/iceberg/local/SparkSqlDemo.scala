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
import org.apache.spark.sql.functions._

/**
 * Spark SQL demo for Iceberg.
 *
 * This example demonstrates:
 * 1. Creating Iceberg tables using Spark SQL DDL
 * 2. Inserting data using DataFrames and SQL
 * 3. Querying data with Spark SQL
 * 4. Advanced operations: joins, aggregations, window functions
 * 5. Iceberg-specific features: snapshots, time travel, schema evolution
 */
object SparkSqlDemo {

  def main(args: Array[String]): Unit = {
    println("=" * 70)
    println("Spark SQL Demo for Apache Iceberg")
    println("=" * 70)
    println()

    // Create Spark session with Iceberg configurations
    val spark = SparkSession.builder()
      .appName("Iceberg Spark SQL Demo")
      .master("local[*]")
      .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.local.type", "hadoop")
      .config("spark.sql.catalog.local.warehouse", "file:///tmp/iceberg-warehouse-spark-sql")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try {
      println("Step 1: Create Database and Tables")
      println("-" * 70)

      // Create database
      spark.sql("CREATE DATABASE IF NOT EXISTS local.ecommerce")
      println("✓ Database created: local.ecommerce")

      // Create customers table
      spark.sql(
        """
          |CREATE TABLE IF NOT EXISTS local.ecommerce.customers (
          |  customer_id BIGINT,
          |  name STRING,
          |  email STRING,
          |  city STRING,
          |  country STRING,
          |  registration_date DATE
          |)
          |USING iceberg
          |PARTITIONED BY (country)
          |""".stripMargin)
      println("✓ Table created: customers (partitioned by country)")

      // Create orders table
      spark.sql(
        """
          |CREATE TABLE IF NOT EXISTS local.ecommerce.orders (
          |  order_id BIGINT,
          |  customer_id BIGINT,
          |  product_name STRING,
          |  quantity INT,
          |  unit_price DOUBLE,
          |  order_date DATE,
          |  status STRING
          |)
          |USING iceberg
          |PARTITIONED BY (days(order_date))
          |""".stripMargin)
      println("✓ Table created: orders (partitioned by days(order_date))")
      println()

      println("Step 2: Insert Data into Tables")
      println("-" * 70)

      // Insert customers data
      spark.sql(
        """
          |INSERT INTO local.ecommerce.customers VALUES
          |  (1, 'Alice Johnson', 'alice@example.com', 'New York', 'USA', DATE'2023-01-15'),
          |  (2, 'Bob Smith', 'bob@example.com', 'London', 'UK', DATE'2023-02-20'),
          |  (3, 'Charlie Brown', 'charlie@example.com', 'Toronto', 'Canada', DATE'2023-03-10'),
          |  (4, 'Diana Prince', 'diana@example.com', 'Sydney', 'Australia', DATE'2023-04-05'),
          |  (5, 'Eve Wilson', 'eve@example.com', 'Berlin', 'Germany', DATE'2023-05-12'),
          |  (6, 'Frank Miller', 'frank@example.com', 'Tokyo', 'Japan', DATE'2023-06-18'),
          |  (7, 'Grace Lee', 'grace@example.com', 'Seoul', 'Korea', DATE'2023-07-22'),
          |  (8, 'Henry Davis', 'henry@example.com', 'Paris', 'France', DATE'2023-08-30')
          |""".stripMargin)
      println("✓ Inserted 8 customers")

      // Insert orders data
      spark.sql(
        """
          |INSERT INTO local.ecommerce.orders VALUES
          |  (101, 1, 'Laptop', 1, 1200.00, DATE'2024-10-01', 'Delivered'),
          |  (102, 2, 'Mouse', 2, 25.00, DATE'2024-10-02', 'Delivered'),
          |  (103, 1, 'Keyboard', 1, 75.00, DATE'2024-10-03', 'Shipped'),
          |  (104, 3, 'Monitor', 2, 300.00, DATE'2024-10-03', 'Delivered'),
          |  (105, 4, 'Laptop', 1, 1200.00, DATE'2024-10-05', 'Processing'),
          |  (106, 2, 'Headphones', 1, 150.00, DATE'2024-10-06', 'Delivered'),
          |  (107, 5, 'Webcam', 3, 80.00, DATE'2024-10-07', 'Shipped'),
          |  (108, 1, 'Mouse', 5, 25.00, DATE'2024-10-08', 'Delivered'),
          |  (109, 6, 'Desk', 1, 350.00, DATE'2024-10-09', 'Processing'),
          |  (110, 3, 'Chair', 2, 150.00, DATE'2024-10-10', 'Shipped')
          |""".stripMargin)
      println("✓ Inserted 10 orders")
      println()

      println("Step 3: Basic Queries")
      println("-" * 70)

      println("\nQuery 1: All customers")
      println("-" * 70)
      spark.sql("SELECT * FROM local.ecommerce.customers ORDER BY customer_id").show()

      println("\nQuery 2: Orders with total amount")
      println("-" * 70)
      spark.sql(
        """
          |SELECT
          |  order_id,
          |  product_name,
          |  quantity,
          |  unit_price,
          |  quantity * unit_price as total_amount,
          |  status
          |FROM local.ecommerce.orders
          |ORDER BY order_id
          |""".stripMargin).show()

      println("\nQuery 3: Customers by country")
      println("-" * 70)
      spark.sql(
        """
          |SELECT country, COUNT(*) as customer_count
          |FROM local.ecommerce.customers
          |GROUP BY country
          |ORDER BY customer_count DESC
          |""".stripMargin).show()

      println("Step 4: Advanced Queries - Joins and Aggregations")
      println("-" * 70)

      println("\nQuery 4: Customer orders with details")
      println("-" * 70)
      spark.sql(
        """
          |SELECT
          |  c.name,
          |  c.country,
          |  o.product_name,
          |  o.quantity,
          |  o.quantity * o.unit_price as amount,
          |  o.status
          |FROM local.ecommerce.customers c
          |JOIN local.ecommerce.orders o ON c.customer_id = o.customer_id
          |ORDER BY c.name, o.order_id
          |""".stripMargin).show(20)

      println("\nQuery 5: Total revenue by customer")
      println("-" * 70)
      spark.sql(
        """
          |SELECT
          |  c.name,
          |  c.country,
          |  COUNT(o.order_id) as total_orders,
          |  SUM(o.quantity * o.unit_price) as total_spent
          |FROM local.ecommerce.customers c
          |LEFT JOIN local.ecommerce.orders o ON c.customer_id = o.customer_id
          |GROUP BY c.customer_id, c.name, c.country
          |ORDER BY total_spent DESC NULLS LAST
          |""".stripMargin).show()

      println("\nQuery 6: Revenue by product")
      println("-" * 70)
      spark.sql(
        """
          |SELECT
          |  product_name,
          |  SUM(quantity) as total_quantity,
          |  SUM(quantity * unit_price) as total_revenue,
          |  AVG(unit_price) as avg_price
          |FROM local.ecommerce.orders
          |GROUP BY product_name
          |ORDER BY total_revenue DESC
          |""".stripMargin).show()

      println("\nQuery 7: Orders by status")
      println("-" * 70)
      spark.sql(
        """
          |SELECT
          |  status,
          |  COUNT(*) as order_count,
          |  SUM(quantity * unit_price) as total_value
          |FROM local.ecommerce.orders
          |GROUP BY status
          |ORDER BY order_count DESC
          |""".stripMargin).show()

      println("Step 5: Window Functions")
      println("-" * 70)

      println("\nQuery 8: Rank customers by spending")
      println("-" * 70)
      spark.sql(
        """
          |SELECT
          |  name,
          |  country,
          |  total_spent,
          |  RANK() OVER (ORDER BY total_spent DESC) as spending_rank,
          |  DENSE_RANK() OVER (PARTITION BY country ORDER BY total_spent DESC) as country_rank
          |FROM (
          |  SELECT
          |    c.customer_id,
          |    c.name,
          |    c.country,
          |    COALESCE(SUM(o.quantity * o.unit_price), 0) as total_spent
          |  FROM local.ecommerce.customers c
          |  LEFT JOIN local.ecommerce.orders o ON c.customer_id = o.customer_id
          |  GROUP BY c.customer_id, c.name, c.country
          |)
          |ORDER BY spending_rank
          |""".stripMargin).show()

      println("Step 6: DataFrame API Operations")
      println("-" * 70)

      // Read tables as DataFrames
      val customersDF = spark.table("local.ecommerce.customers")
      val ordersDF = spark.table("local.ecommerce.orders")

      println("\nDataFrame Operation 1: Filter and select")
      ordersDF
        .filter(col("quantity") > 1)
        .select("order_id", "product_name", "quantity", "status")
        .orderBy(col("quantity").desc)
        .show()

      println("\nDataFrame Operation 2: Complex aggregation")
      ordersDF
        .withColumn("total_amount", col("quantity") * col("unit_price"))
        .groupBy("status")
        .agg(
          count("order_id").as("count"),
          sum("total_amount").as("total"),
          avg("total_amount").as("average")
        )
        .orderBy(col("total").desc)
        .show()

      println("\nDataFrame Operation 3: Join with aggregation")
      customersDF
        .join(ordersDF, Seq("customer_id"), "left")
        .groupBy("name", "country")
        .agg(
          count("order_id").as("orders"),
          sum(col("quantity") * col("unit_price")).as("revenue")
        )
        .na.fill(0)
        .orderBy(col("revenue").desc)
        .show()

      println("Step 7: Iceberg-Specific Features")
      println("-" * 70)

      println("\nTable snapshots:")
      spark.sql("SELECT snapshot_id, committed_at, operation FROM local.ecommerce.orders.snapshots")
        .show(false)

      println("\nTable metadata:")
      println(s"Customers table location: ${spark.table("local.ecommerce.customers").inputFiles.mkString(", ")}")

      println()
      println("=" * 70)
      println("Demo completed successfully!")
      println("=" * 70)
      println()
      println("Data location: /tmp/iceberg-warehouse-spark-sql/ecommerce/")
      println("You can explore the data:")
      println("  1. spark.sql(\"SHOW TABLES IN local.ecommerce\").show()")
      println("  2. spark.sql(\"DESCRIBE TABLE local.ecommerce.customers\").show()")
      println("  3. spark.sql(\"SELECT * FROM local.ecommerce.orders\").show()")

    } finally {
      spark.stop()
    }
  }
}
