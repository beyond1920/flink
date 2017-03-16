/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.sources

import org.apache.flink.table.annotation.TableType
import org.apache.flink.table.catalog.{ExternalCatalogTable, TableSourceConverter}

import scala.collection.JavaConverters._
import java.util.{Set => JSet}

import com.google.common.collect.ImmutableSet

/**
  * The class defines a converter used to convert [[CsvTableSource]] to
  * or from [[ExternalCatalogTable]].
  */
@TableType(value = "csv")
class CsvTableSourceConverter extends TableSourceConverter[CsvTableSource] {

  private val required: JSet[String] = ImmutableSet.of("path")

  override def requiredProperties: JSet[String] = required

  override def fromExternalCatalogTable(
      externalCatalogTable: ExternalCatalogTable): CsvTableSource = {
    val params = externalCatalogTable.properties.asScala
    val csvTableSourceBuilder = new CsvTableSource.Builder

    params.get("path").foreach(csvTableSourceBuilder.path)
    params.get("fieldDelim").foreach(csvTableSourceBuilder.fieldDelimiter)
    params.get("rowDelim").foreach(csvTableSourceBuilder.lineDelimiter)
    params.get("quoteCharacter").foreach(quoteStr =>
      if (quoteStr.length != 1) {
        throw new IllegalArgumentException("the value of param quoteCharacter is invalid")
      } else {
        csvTableSourceBuilder.quoteCharacter(quoteStr.charAt(0))
      }
    )
    params.get("ignoreFirstLine").foreach(ignoreFirstLineStr =>
        if(ignoreFirstLineStr.toBoolean) {
          csvTableSourceBuilder.ignoreFirstLine()
        }
    )
    params.get("ignoreComments").foreach(csvTableSourceBuilder.commentPrefix)
    params.get("lenient").foreach(lenientStr =>
        if(lenientStr.toBoolean) {
          csvTableSourceBuilder.ignoreParseErrors
        }
    )
    externalCatalogTable.schema.columnNames
        .zip(externalCatalogTable.schema.columnTypes)
        .foreach(field => csvTableSourceBuilder.field(field._1, field._2))

    csvTableSourceBuilder.build()
  }

}
