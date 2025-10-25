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

import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.hadoop.fs.Path

import java.io.File

object ReadAvroFileSuite {
  def main(args: Array[String]): Unit = {
    // Avro 文件路径
    val avroFilePath = "/Users/wenzhilong/warehouse/space/iceberg/files/iceberg_warehouse/default/user_behavior_local/metadata/snap-2515384077258008070-1-b863bff4-ce2e-44e4-ab68-375b6c233996.avro"

    println(s"正在读取 Avro 文件: $avroFilePath")
    println("=" * 80)

    // 创建 Avro reader
    val file = new File(avroFilePath)
    if (!file.exists()) {
      println(s"错误: 文件不存在 - $avroFilePath")
      return
    }

    val datumReader = new GenericDatumReader[GenericRecord]()
    val dataFileReader = DataFileReader.openReader(file, datumReader)

    try {
      // 打印 Schema
      println("Avro Schema:")
      println(dataFileReader.getSchema.toString(true))
      println("=" * 80)

      // 读取并打印所有记录
      println("Avro 文件内容:")
      println("-" * 80)

      var recordCount = 0
      while (dataFileReader.hasNext) {
        val record = dataFileReader.next()
        recordCount += 1

        println(s"\n记录 #$recordCount:")
        printRecord(record, "  ")
      }

      println("\n" + "=" * 80)
      println(s"总共读取了 $recordCount 条记录")

    } finally {
      dataFileReader.close()
    }
  }

  /**
   * 递归打印 GenericRecord 的内容
   */
  private def printRecord(record: GenericRecord, indent: String): Unit = {
    val schema = record.getSchema
    import scala.collection.JavaConverters._

    schema.getFields.asScala.foreach { field =>
      val fieldName = field.name()
      val value = record.get(fieldName)

      value match {
        case null =>
          println(s"$indent$fieldName: null")
        case nested: GenericRecord =>
          println(s"$indent$fieldName: {")
          printRecord(nested, indent + "  ")
          println(s"$indent}")
        case list: java.util.List[_] =>
          println(s"$indent$fieldName: [")
          list.asScala.zipWithIndex.foreach { case (item, idx) =>
            item match {
              case nestedRecord: GenericRecord =>
                println(s"$indent  [$idx]: {")
                printRecord(nestedRecord, indent + "    ")
                println(s"$indent  }")
              case _ =>
                println(s"$indent  [$idx]: $item")
            }
          }
          println(s"$indent]")
        case map: java.util.Map[_, _] =>
          println(s"$indent$fieldName: {")
          map.asScala.foreach { case (k, v) =>
            println(s"$indent  $k: $v")
          }
          println(s"$indent}")
        case bytes: java.nio.ByteBuffer =>
          val byteArray = new Array[Byte](bytes.remaining())
          bytes.duplicate().get(byteArray)
          println(s"$indent$fieldName: [ByteBuffer, ${byteArray.length} bytes]")
        case _ =>
          println(s"$indent$fieldName: $value")
      }
    }
  }
}
