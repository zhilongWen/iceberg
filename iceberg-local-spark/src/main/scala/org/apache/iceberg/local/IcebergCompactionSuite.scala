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
import org.apache.iceberg.Table
import org.apache.iceberg.catalog.{Namespace, TableIdentifier}
import org.apache.iceberg.hadoop.HadoopCatalog
import org.apache.iceberg.spark.actions.SparkActions
import org.apache.iceberg.actions.RewriteDataFiles
import org.apache.spark.sql.SparkSession
import java.util.concurrent.TimeUnit
import java.text.SimpleDateFormat
import java.util.Date

import scala.collection.JavaConverters._

object IcebergCompactionSuite {
  def main(args: Array[String]): Unit = {
    println("=" * 100)
    println("开始执行 Iceberg 表压缩和维护操作")
    println("=" * 100)

    // 1. 初始化 SparkSession（SparkActions 需要 Spark）
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName(this.getClass.getSimpleName)
      .config("spark.sql.catalog.iceberg_local", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.iceberg_local.type", "hadoop")
      .config("spark.sql.catalog.iceberg_local.warehouse", "file:///Users/wenzhilong/warehouse/space/iceberg/files/iceberg_warehouse")
      .getOrCreate()

    try {
      // 2. 初始化 Hadoop 配置和 Catalog
      val conf = new Configuration()
      val warehousePath = "file:///Users/wenzhilong/warehouse/space/iceberg/files/iceberg_warehouse"
      val hadoopCatalog = new HadoopCatalog(conf, warehousePath)

      // 3. 加载 Iceberg 表
      val tableIdentifier = TableIdentifier.of("default", "user_behavior_local")
      println(s"\n正在加载表: ${tableIdentifier}")
      val table: Table = hadoopCatalog.loadTable(tableIdentifier)
      println(s"表加载成功: ${table.location()}")

      // 4. 打印表的基本信息
      printTableInfo(table)

      // 5. 打印表的历史快照
      printTableHistory(table)

      // 6. 打印当前快照详细信息
      printCurrentSnapshotDetails(table)

      // 7. 打印表的所有数据文件信息
      printDataFiles(table)

      // 8. 执行小文件合并（使用 SparkActions）
      println("\n" + "=" * 100)
      println("开始执行小文件合并...")
      println("=" * 100)

      val targetFileSize = 128 * 1024 * 1024L // 128MB
      val compactionResult = SparkActions.get(spark)
        .rewriteDataFiles(table)
        .option(RewriteDataFiles.TARGET_FILE_SIZE_BYTES, targetFileSize.toString)
        .option("rewrite-all", "false") // 只重写小文件
        .execute()

      println(s"小文件合并完成:")
      println(s"  - 重写的数据文件数: ${compactionResult.rewrittenDataFilesCount()}")
      println(s"  - 新增的数据文件数: ${compactionResult.addedDataFilesCount()}")
      println(s"  - 重写的字节数: ${compactionResult.rewrittenBytesCount() / (1024 * 1024)} MB")

      // 9. 打印合并后的数据文件信息
      table.refresh() // 刷新表元数据
      println("\n合并后的数据文件信息:")
      printDataFiles(table)

      // 10. 清理过期快照（保留最近 7 天的快照）
      println("\n" + "=" * 100)
      println("开始清理过期快照...")
      println("=" * 100)

      val retainDays = 7
      val expireTimestamp = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(retainDays)

      println(s"清理 ${retainDays} 天前的快照 (时间戳早于: ${new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(expireTimestamp))})")

      val expireSnapshotsResult = SparkActions.get(spark)
        .expireSnapshots(table)
        .expireOlderThan(expireTimestamp)
        .execute()

      println(s"过期快照清理完成，删除的数据文件数: ${expireSnapshotsResult.deletedDataFilesCount()}")

      // 11. 清理孤儿文件（不再被任何快照引用的文件）
      println("\n" + "=" * 100)
      println("开始清理孤儿文件...")
      println("=" * 100)

      val olderThanTimestamp = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1) // 清理 1 天前的孤儿文件

      val orphanFilesResult = SparkActions.get(spark)
        .deleteOrphanFiles(table)
        .olderThan(olderThanTimestamp)
        .execute()

      println(s"孤儿文件清理完成")
      println(s"  - 删除的孤儿文件:")
      orphanFilesResult.orphanFileLocations().asScala.take(10).foreach { file =>
        println(s"    - $file")
      }

      // 12. 打印最终的表信息
      println("\n" + "=" * 100)
      println("维护操作完成后的表信息")
      println("=" * 100)
      table.refresh()
      printTableInfo(table)

      println("\n" + "=" * 100)
      println("所有维护操作执行完成！")
      println("=" * 100)

    } catch {
      case e: Exception =>
        println(s"\n错误: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }

  /**
   * 打印表的基本信息
   */
  private def printTableInfo(table: Table): Unit = {
    println("\n" + "=" * 100)
    println("表的基本信息")
    println("=" * 100)
    println(s"表名: ${table.name()}")
    println(s"表位置: ${table.location()}")
    println(s"Schema: ${table.schema()}")
    println(s"分区规范: ${table.spec()}")
    println(s"排序顺序: ${table.sortOrder()}")
    println(s"属性: ${table.properties().asScala.mkString(", ")}")
  }

  /**
   * 打印表的历史快照
   */
  private def printTableHistory(table: Table): Unit = {
    println("\n" + "=" * 100)
    println("表的历史快照")
    println("=" * 100)

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    table.history().asScala.foreach { historyEntry =>
      val timestamp = new Date(historyEntry.timestampMillis())
      val snapshot = table.snapshot(historyEntry.snapshotId())
      val operation = if (snapshot != null) snapshot.operation() else "unknown"
      println(f"快照 ID: ${historyEntry.snapshotId()}%20d | " +
        f"时间: ${dateFormat.format(timestamp)} | " +
        f"操作: $operation")
    }
  }

  /**
   * 打印当前快照的详细信息
   */
  private def printCurrentSnapshotDetails(table: Table): Unit = {
    println("\n" + "=" * 100)
    println("当前活跃快照详细信息")
    println("=" * 100)

    val currentSnapshot = table.currentSnapshot()
    if (currentSnapshot != null) {
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val timestamp = new Date(currentSnapshot.timestampMillis())

      println(s"快照 ID: ${currentSnapshot.snapshotId()}")
      println(s"时间戳: ${dateFormat.format(timestamp)}")
      println(s"操作: ${currentSnapshot.operation()}")
      println(s"数据 Manifest 数: ${currentSnapshot.dataManifests(table.io()).size()}")
      println(s"Manifest 列表: ${currentSnapshot.manifestListLocation()}")
      println(s"摘要: ${currentSnapshot.summary().asScala.mkString(", ")}")
    } else {
      println("当前没有活跃快照")
    }
  }

  /**
   * 打印表的所有数据文件信息
   */
  private def printDataFiles(table: Table): Unit = {
    println("\n" + "=" * 100)
    println("表的数据文件信息")
    println("=" * 100)

    import org.apache.iceberg.io.CloseableIterable

    val dataFiles = table.newScan().planFiles()
    val dataFileList = dataFiles.asScala.toList

    println(s"总数据文件数: ${dataFileList.size}")

    if (dataFileList.nonEmpty) {
      println("\n数据文件详情:")
      dataFileList.zipWithIndex.foreach { case (fileScanTask, idx) =>
        val file = fileScanTask.file()
        val sizeInMB = file.fileSizeInBytes() / (1024.0 * 1024.0)
        println(f"  [$idx%3d] ${file.path()}%-80s | 大小: ${sizeInMB}%8.2f MB | 记录数: ${file.recordCount()}%6d")
      }

      // 统计信息
      val totalSize = dataFileList.map(_.file().fileSizeInBytes()).sum
      val totalRecords = dataFileList.map(_.file().recordCount()).sum
      val avgSize = totalSize.toDouble / dataFileList.size / (1024 * 1024)

      println("\n统计信息:")
      println(f"  - 总大小: ${totalSize / (1024.0 * 1024.0)}%.2f MB")
      println(f"  - 总记录数: $totalRecords")
      println(f"  - 平均文件大小: $avgSize%.2f MB")
    }
  }
}
