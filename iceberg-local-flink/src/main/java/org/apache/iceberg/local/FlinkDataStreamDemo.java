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

import java.util.Map;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.flink.source.FlinkSource;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;

/**
 * Flink DataStream API demo for Iceberg.
 *
 * <p>This example demonstrates:
 * 1. Creating an Iceberg table with Hadoop catalog
 * 2. Writing data to Iceberg table using DataStream API
 * 3. Reading data from Iceberg table using DataStream API
 */
public class FlinkDataStreamDemo {

  public static void main(String[] args) throws Exception {
    // Set up the execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    // Define warehouse path
    String warehousePath = "file:///tmp/iceberg-warehouse";

    // Create Hadoop catalog
    Configuration hadoopConf = new Configuration();
    HadoopCatalog catalog = new HadoopCatalog(hadoopConf, warehousePath);

    // Define table identifier
    TableIdentifier tableId = TableIdentifier.of(Namespace.of("default"), "user_events");

    // Define schema
    Schema schema = new Schema(
        Types.NestedField.required(1, "user_id", Types.LongType.get()),
        Types.NestedField.required(2, "user_name", Types.StringType.get()),
        Types.NestedField.required(3, "event_type", Types.StringType.get()),
        Types.NestedField.required(4, "event_time", Types.LongType.get())
    );

    // Create table if not exists
    Table table;
    if (!catalog.tableExists(tableId)) {
      PartitionSpec spec = PartitionSpec.builderFor(schema)
          .identity("event_type")
          .build();

      table = catalog.createTable(tableId, schema, spec);
      System.out.println("Created table: " + tableId);
    } else {
      table = catalog.loadTable(tableId);
      System.out.println("Loaded existing table: " + tableId);
    }

    // Create TableLoader
    TableLoader tableLoader = TableLoader.fromHadoopTable(table.location(), hadoopConf);

    // ==================== Writing Data ====================
    System.out.println("\n=== Writing data to Iceberg table ===");

    // Create sample data stream
    DataStream<RowData> dataStream = env.fromElements(
        createRowData(1L, "Alice", "login", System.currentTimeMillis()),
        createRowData(2L, "Bob", "click", System.currentTimeMillis()),
        createRowData(3L, "Charlie", "login", System.currentTimeMillis()),
        createRowData(4L, "David", "purchase", System.currentTimeMillis()),
        createRowData(5L, "Eve", "click", System.currentTimeMillis())
    );

    // Write to Iceberg table
    FlinkSink.forRowData(dataStream)
        .table(table)
        .tableLoader(tableLoader)
        .append();

    // Execute the write job
    env.execute("Flink DataStream Write to Iceberg");

    System.out.println("\n=== Data written successfully ===");
    System.out.println("Table location: " + table.location());
    System.out.println("\nYou can verify the data by:");
    System.out.println("1. Checking the warehouse directory: /tmp/iceberg-warehouse");
    System.out.println("2. Using Flink SQL to query the table");
    System.out.println("3. Reading with a separate application");

    System.out.println("\n=== Demo completed successfully ===");
  }

  /**
   * Helper method to create RowData.
   */
  private static RowData createRowData(Long userId, String userName, String eventType, Long eventTime) {
    GenericRowData rowData = new GenericRowData(4);
    rowData.setField(0, userId);
    rowData.setField(1, StringData.fromString(userName));
    rowData.setField(2, StringData.fromString(eventType));
    rowData.setField(3, eventTime);
    return rowData;
  }
}
