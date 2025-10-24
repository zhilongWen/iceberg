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

package org.apache.iceberg.local;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Flink SQL demo for Iceberg.
 *
 * <p>This example demonstrates:
 * 1. Creating an Iceberg catalog in Flink SQL
 * 2. Creating tables using DDL
 * 3. Inserting data using SQL INSERT
 * 4. Querying data using SQL SELECT
 * 5. Performing aggregations and joins
 */
public class FlinkSqlDemo {

  public static void main(String[] args) throws Exception {
    // Set up the execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    // Create table environment in batch mode for queries
    EnvironmentSettings settings = EnvironmentSettings.newInstance()
        .inBatchMode()
        .build();
    TableEnvironment tableEnv = TableEnvironment.create(settings);

    System.out.println("=== Flink SQL Demo for Apache Iceberg ===\n");

    // ==================== Create Catalog ====================
    System.out.println("Step 1: Creating Iceberg catalog");
    String createCatalog = "CREATE CATALOG iceberg_catalog WITH (\n" +
        "  'type' = 'iceberg',\n" +
        "  'catalog-type' = 'hadoop',\n" +
        "  'warehouse' = 'file:///tmp/iceberg-warehouse-sql',\n" +
        "  'property-version' = '1'\n" +
        ")";
    tableEnv.executeSql(createCatalog);
    System.out.println("✓ Catalog created\n");

    // Use the catalog
    tableEnv.executeSql("USE CATALOG iceberg_catalog");
    System.out.println("✓ Using iceberg_catalog\n");

    // Create database
    tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS demo_db");
    tableEnv.executeSql("USE demo_db");
    System.out.println("✓ Database demo_db created and selected\n");

    // ==================== Create Tables ====================
    System.out.println("Step 2: Creating tables");

    // Drop tables if they exist
    try {
      tableEnv.executeSql("DROP TABLE IF EXISTS products");
      tableEnv.executeSql("DROP TABLE IF EXISTS orders");
    } catch (Exception e) {
      // Ignore if tables don't exist
    }

    // Create products table
    String createProductsTable = "CREATE TABLE products (\n" +
        "  product_id BIGINT,\n" +
        "  product_name STRING,\n" +
        "  category STRING,\n" +
        "  price DOUBLE\n" +
        ") PARTITIONED BY (category)\n" +
        "WITH (\n" +
        "  'format-version' = '2'\n" +
        ")";
    tableEnv.executeSql(createProductsTable);
    System.out.println("✓ Products table created\n");

    // Create orders table
    String createOrdersTable = "CREATE TABLE orders (\n" +
        "  order_id BIGINT,\n" +
        "  product_id BIGINT,\n" +
        "  quantity INT,\n" +
        "  order_time TIMESTAMP(3),\n" +
        "  order_date STRING\n" +
        ") PARTITIONED BY (order_date)\n" +
        "WITH (\n" +
        "  'format-version' = '2'\n" +
        ")";
    tableEnv.executeSql(createOrdersTable);
    System.out.println("✓ Orders table created\n");

    // ==================== Insert Data ====================
    System.out.println("Step 3: Inserting data into tables");

    // Insert products
    String insertProducts = "INSERT INTO products VALUES\n" +
        "  (1, 'Laptop', 'Electronics', 1200.00),\n" +
        "  (2, 'Mouse', 'Electronics', 25.00),\n" +
        "  (3, 'Desk', 'Furniture', 350.00),\n" +
        "  (4, 'Chair', 'Furniture', 150.00),\n" +
        "  (5, 'Monitor', 'Electronics', 300.00)";
    TableResult insertResult = tableEnv.executeSql(insertProducts);
    insertResult.await();
    System.out.println("✓ Products data inserted\n");

    // Insert orders
    String insertOrders = "INSERT INTO orders VALUES\n" +
        "  (101, 1, 2, TIMESTAMP '2024-10-20 10:30:00', '2024-10-20'),\n" +
        "  (102, 2, 5, TIMESTAMP '2024-10-20 11:15:00', '2024-10-20'),\n" +
        "  (103, 3, 1, TIMESTAMP '2024-10-21 09:00:00', '2024-10-21'),\n" +
        "  (104, 1, 1, TIMESTAMP '2024-10-21 14:30:00', '2024-10-21'),\n" +
        "  (105, 5, 3, TIMESTAMP '2024-10-22 16:45:00', '2024-10-22')";
    insertResult = tableEnv.executeSql(insertOrders);
    insertResult.await();
    System.out.println("✓ Orders data inserted\n");

    // ==================== Query Data ====================
    System.out.println("Step 4: Querying data\n");

    // Query 1: Select all products
    System.out.println("Query 1: All products");
    System.out.println("----------------------------------------");
    TableResult result1 = tableEnv.executeSql("SELECT * FROM products ORDER BY product_id");
    result1.print();
    System.out.println();

    // Query 2: Select products by category
    System.out.println("Query 2: Electronics products");
    System.out.println("----------------------------------------");
    TableResult result2 = tableEnv.executeSql(
        "SELECT product_id, product_name, price FROM products WHERE category = 'Electronics' ORDER BY price DESC"
    );
    result2.print();
    System.out.println();

    // Query 3: Aggregation - Count products by category
    System.out.println("Query 3: Product count by category");
    System.out.println("----------------------------------------");
    TableResult result3 = tableEnv.executeSql(
        "SELECT category, COUNT(*) as product_count, AVG(price) as avg_price " +
        "FROM products GROUP BY category"
    );
    result3.print();
    System.out.println();

    // Query 4: Join products and orders
    System.out.println("Query 4: Order details with product information");
    System.out.println("----------------------------------------");
    TableResult result4 = tableEnv.executeSql(
        "SELECT " +
        "  o.order_id, " +
        "  p.product_name, " +
        "  p.category, " +
        "  o.quantity, " +
        "  p.price, " +
        "  o.quantity * p.price as total_price " +
        "FROM orders o " +
        "JOIN products p ON o.product_id = p.product_id " +
        "ORDER BY o.order_id"
    );
    result4.print();
    System.out.println();

    // Query 5: Aggregation on joined data
    System.out.println("Query 5: Total revenue by category");
    System.out.println("----------------------------------------");
    TableResult result5 = tableEnv.executeSql(
        "SELECT " +
        "  p.category, " +
        "  SUM(o.quantity * p.price) as total_revenue, " +
        "  COUNT(o.order_id) as order_count " +
        "FROM orders o " +
        "JOIN products p ON o.product_id = p.product_id " +
        "GROUP BY p.category " +
        "ORDER BY total_revenue DESC"
    );
    result5.print();
    System.out.println();

    System.out.println("\n=== Demo completed successfully ===");
    System.out.println("\nData has been successfully:");
    System.out.println("- Created in catalog: iceberg_catalog");
    System.out.println("- Stored in warehouse: /tmp/iceberg-warehouse-sql");
    System.out.println("- Partitioned for efficient querying");
    System.out.println("\nYou can explore the data:");
    System.out.println("- Check warehouse: ls -la /tmp/iceberg-warehouse-sql/demo_db/");
    System.out.println("- Query using Flink SQL again");
    System.out.println("- Access via other engines (Spark, Trino, etc.)");
  }
}
