# Iceberg Local Flink Demo

This module contains demo applications showing how to use Apache Iceberg with Apache Flink 1.17.

## Features

This demo project includes:

1. **Flink DataStream API Demo** - Shows how to read and write Iceberg tables using the DataStream API
2. **Flink SQL Demo** - Demonstrates creating, inserting, and querying Iceberg tables using Flink SQL

## Prerequisites

- Java 8, 11, or 17
- Gradle (wrapper included)

## Building

Build the module:

```bash
./gradlew :iceberg-local-flink:build
```

Compile only:

```bash
./gradlew :iceberg-local-flink:compileJava
```

## Running the Demos

### DataStream API Demo

The DataStream demo creates an Iceberg table with Hadoop catalog and demonstrates:
- Creating tables programmatically
- Writing data using DataStream API
- Reading data using DataStream API

Run the DataStream demo:

```bash
./gradlew :iceberg-local-flink:run --args="datastream"
```

**What it does:**
- Creates a `user_events` table with schema: user_id, user_name, event_type, event_time
- Partitions by `event_type`
- Writes 5 sample records
- Reads and prints the data

**Location:** `org.apache.iceberg.local.FlinkDataStreamDemo`

### SQL Demo

The SQL demo showcases Flink SQL capabilities with Iceberg:
- Creating Iceberg catalog
- Creating tables with DDL
- Inserting data
- Running various queries (SELECT, JOIN, GROUP BY)
- Time travel queries (snapshot queries)

Run the SQL demo:

```bash
./gradlew :iceberg-local-flink:run --args="sql"
```

**What it does:**
- Creates an Iceberg catalog with Hadoop backend
- Creates `products` and `orders` tables
- Inserts sample data
- Runs multiple queries including joins and aggregations
- Shows available snapshots for time travel

**Location:** `org.apache.iceberg.local.FlinkSqlDemo`

## Demo Details

### FlinkDataStreamDemo.java

```java
// Key operations shown:
1. Creating Hadoop Catalog
2. Defining Iceberg Schema
3. Creating partitioned table
4. Writing RowData using FlinkSink
5. Reading data using FlinkSource
```

**Schema:**
- user_id (LONG) - Required
- user_name (STRING) - Required
- event_type (STRING) - Required, partition key
- event_time (LONG) - Required

**Output Location:** `/tmp/iceberg-warehouse`

### FlinkSqlDemo.java

```sql
-- Key operations shown:
1. CREATE CATALOG (Iceberg with Hadoop backend)
2. CREATE TABLE (with partitioning)
3. INSERT INTO (batch insert)
4. SELECT (various queries)
5. JOIN (between tables)
6. GROUP BY (aggregations)
7. Time travel (snapshot queries)
```

**Tables:**
- `products` - product_id, product_name, category, price (partitioned by category)
- `orders` - order_id, product_id, quantity, order_time (partitioned by date)

**Output Location:** `/tmp/iceberg-warehouse-sql`

## Project Structure

```
iceberg-local-flink/
├── build.gradle                    # Build configuration
├── README.md                       # This file
└── src/
    └── main/
        └── java/
            └── org/apache/iceberg/local/
                ├── Main.java                   # Entry point
                ├── FlinkDataStreamDemo.java    # DataStream API demo
                └── FlinkSqlDemo.java           # SQL demo
```

## Dependencies

This module depends on:
- `iceberg-api`, `iceberg-core`, `iceberg-data` - Core Iceberg libraries
- `iceberg-flink:iceberg-flink-1.17` - Iceberg Flink integration
- `flink-streaming-java`, `flink-table-*` - Flink 1.17 libraries
- `hadoop-common`, `hadoop-hdfs` - Hadoop file system support

## Viewing Results

After running the demos, you can inspect the warehouse directories:

```bash
# DataStream demo output
ls -la /tmp/iceberg-warehouse/default/user_events/

# SQL demo output
ls -la /tmp/iceberg-warehouse-sql/demo_db/
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

## Learning Resources

- [Apache Iceberg Documentation](https://iceberg.apache.org/docs/latest/)
- [Flink Iceberg Integration](https://iceberg.apache.org/docs/latest/flink/)
- [Flink DataStream API](https://nightlies.apache.org/flink/flink-docs-release-1.17/)
- [Flink SQL](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/table/sql/overview/)

## Next Steps

To extend these demos:
1. Add more complex partitioning strategies
2. Implement streaming writes with checkpoint
3. Add Schema evolution examples
4. Demonstrate time travel queries
5. Integrate with cloud storage (S3, GCS, Azure)
6. Add CDC (Change Data Capture) examples

## License

Licensed under the Apache License, Version 2.0. See the LICENSE file in the project root for details.
