# Iceberg Local Spark Demo

This module contains demo applications showing how to use Apache Iceberg with Apache Spark 3.4 and Scala 2.12.

## Features

This demo project includes:

1. **Spark RDD API Demo** - Shows how to use Spark RDD operations with Iceberg tables
2. **Spark SQL Demo** - Demonstrates comprehensive Spark SQL operations with Iceberg

## Prerequisites

- Java 8, 11, or 17
- Gradle (wrapper included)
- Scala 2.12.17 (automatically managed by Gradle)

## Building

Build the module:

```bash
./gradlew :iceberg-local-spark:build
```

Compile only:

```bash
./gradlew :iceberg-local-spark:compileScala
```

## Running the Demos

### RDD API Demo

The RDD demo showcases:
- Creating Iceberg tables programmatically
- RDD transformations (map, filter, reduce)
- Writing RDD data to Iceberg tables
- Reading Iceberg data as RDD
- Complex RDD operations (mapPartitions, reduceByKey)
- Revenue and aggregation calculations using RDD operations

Run the RDD demo:

```bash
./gradlew :iceberg-local-spark:run --args="rdd"
```

**What it does:**
- Creates a `sales_data` table with schema: sale_id, product, quantity, price, region
- Partitions by `region`
- Processes 10 sales transactions using RDD operations
- Performs 5 different RDD operations:
  1. Filter by region
  2. Calculate revenue by product
  3. Calculate quantity by region
  4. Find high-quantity orders
  5. Count sales by product using mapPartitions

**Output Location:** `/tmp/iceberg-warehouse-spark-rdd/default/sales_data`

### SQL Demo

The SQL demo showcases comprehensive Spark SQL features:
- Creating databases and tables with DDL
- INSERT operations
- Basic and advanced SELECT queries
- JOIN operations (inner join, left join)
- Aggregations (COUNT, SUM, AVG)
- Window functions (RANK, DENSE_RANK)
- DataFrame API operations
- Iceberg metadata queries (snapshots)

Run the SQL demo:

```bash
./gradlew :iceberg-local-spark:run --args="sql"
```

**What it does:**
- Creates database `local.ecommerce`
- Creates two tables:
  - `customers` - customer_id, name, email, city, country, registration_date (partitioned by country)
  - `orders` - order_id, customer_id, product_name, quantity, unit_price, order_date, status (partitioned by days)
- Inserts 8 customers and 10 orders
- Runs 8 SQL queries demonstrating:
  - Simple queries
  - Filtering and aggregations
  - Multi-table joins
  - Window functions
  - DataFrame API operations

**Output Location:** `/tmp/iceberg-warehouse-spark-sql/ecommerce/`

## Demo Details

### SparkRddDemo.scala

**Key operations:**
```scala
// Create RDD from data
val salesRDD = spark.sparkContext.parallelize(salesData)

// Transform data
val transformedRDD = salesRDD.map { case (id, product, qty, price, region) =>
  val total = qty * price
  Row(id, product, qty, price, region)
}

// Convert to DataFrame and write to Iceberg
val salesDF = spark.createDataFrame(transformedRDD, sparkSchema)
salesDF.writeTo("hadoop_catalog.default.sales_data").append()

// Read and process with RDD operations
val dataRDD = spark.table("hadoop_catalog.default.sales_data").rdd
val revenueByProduct = dataRDD
  .map { row => (product, quantity * price) }
  .reduceByKey(_ + _)
```

**Schema:**
- sale_id (LONG)
- product (STRING)
- quantity (INT)
- price (DOUBLE)
- region (STRING) - Partition key

### SparkSqlDemo.scala

**Key operations:**
```sql
-- Create table
CREATE TABLE local.ecommerce.customers (
  customer_id BIGINT,
  name STRING,
  email STRING,
  city STRING,
  country STRING,
  registration_date DATE
)
USING iceberg
PARTITIONED BY (country)

-- Insert data
INSERT INTO local.ecommerce.customers VALUES
  (1, 'Alice Johnson', 'alice@example.com', 'New York', 'USA', DATE'2023-01-15'),
  ...

-- Query with joins
SELECT c.name, c.country, o.product_name, o.quantity
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id

-- Window functions
SELECT name, country, total_spent,
  RANK() OVER (ORDER BY total_spent DESC) as spending_rank
FROM customer_revenue
```

**DataFrame API:**
```scala
// Filter and select
ordersDF
  .filter(col("quantity") > 1)
  .select("order_id", "product_name", "quantity")
  .orderBy(col("quantity").desc)

// Aggregations
ordersDF
  .withColumn("total_amount", col("quantity") * col("unit_price"))
  .groupBy("status")
  .agg(count("order_id"), sum("total_amount"))
```

## Project Structure

```
iceberg-local-spark/
├── build.gradle                                    # Build configuration
├── README.md                                       # This file
└── src/
    └── main/
        └── scala/
            └── org/apache/iceberg/local/
                ├── Main.scala                      # Entry point
                ├── SparkRddDemo.scala             # RDD API demo
                └── SparkSqlDemo.scala             # SQL demo
```

## Dependencies

This module uses:
- Scala 2.12.17
- Spark 3.4.3
- Iceberg core libraries
- Iceberg Spark 3.4 integration
- Hadoop 2.7.3 for file system support

## Viewing Results

After running the demos, you can inspect the warehouse directories:

```bash
# RDD demo output
ls -la /tmp/iceberg-warehouse-spark-rdd/default/sales_data/

# SQL demo output
ls -la /tmp/iceberg-warehouse-spark-sql/ecommerce/
```

You can also use Spark SQL to query the tables:

```scala
spark-shell --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.7.0-SNAPSHOT

val spark = SparkSession.builder()
  .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
  .config("spark.sql.catalog.local.type", "hadoop")
  .config("spark.sql.catalog.local.warehouse", "file:///tmp/iceberg-warehouse-spark-sql")
  .getOrCreate()

spark.sql("SHOW TABLES IN local.ecommerce").show()
spark.sql("SELECT * FROM local.ecommerce.customers").show()
```

## Troubleshooting

### Permission Issues
If you encounter permission errors with `/tmp/iceberg-warehouse*`, ensure the directories are writable:

```bash
sudo chmod -R 777 /tmp/iceberg-warehouse*
```

### Memory Issues
If you encounter OutOfMemoryError, increase Gradle JVM memory in `gradle.properties`:

```properties
org.gradle.jvmargs=-Xmx4g -XX:MaxMetaspaceSize=512m
```

### Compilation Issues
If you encounter Scala compilation issues, ensure you have the correct Scala version:

```bash
./gradlew :iceberg-local-spark:dependencies --configuration compileClasspath | grep scala
```

## Learning Resources

- [Apache Iceberg Documentation](https://iceberg.apache.org/docs/latest/)
- [Iceberg Spark Integration](https://iceberg.apache.org/docs/latest/spark-getting-started/)
- [Spark SQL Guide](https://spark.apache.org/docs/3.4.0/sql-programming-guide.html)
- [Spark RDD Programming Guide](https://spark.apache.org/docs/3.4.0/rdd-programming-guide.html)
- [Scala Documentation](https://docs.scala-lang.org/tour/tour-of-scala.html)

## Next Steps

To extend these demos:
1. Add more complex transformations with RDD operations
2. Implement custom partitioning strategies
3. Add time travel query examples
4. Demonstrate schema evolution
5. Integrate with Delta Lake or other formats
6. Add performance tuning examples
7. Demonstrate CDC (Change Data Capture)
8. Add streaming examples with Structured Streaming

## License

Licensed under the Apache License, Version 2.0. See the LICENSE file in the project root for details.
