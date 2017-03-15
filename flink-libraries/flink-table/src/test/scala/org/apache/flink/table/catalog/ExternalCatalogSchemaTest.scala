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

package org.apache.flink.table.catalog

import java.util.Collections

import com.google.common.collect.Lists
import org.apache.calcite.jdbc.CalciteSchema
import org.apache.calcite.prepare.CalciteCatalogReader
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.sql.validate.SqlMonikerType
import org.apache.commons.collections.CollectionUtils
import org.apache.flink.table.calcite.{FlinkTypeFactory, FlinkTypeSystem}
import org.apache.flink.table.plan.schema.TableSourceTable
import org.apache.flink.table.sources.CsvTableSource
import org.apache.flink.table.utils.CommonTestData
import org.junit.{Before, Test}
import org.junit.Assert._

class ExternalCatalogSchemaTest {

  private val schemaName: String = "test"
  private var externalCatalogSchema: ExternalCatalogSchema = _
  private var calciteCatalogReader: CalciteCatalogReader = _
  private val db = "db1"
  private val tb = "tb1"

  @Before
  def setUp(): Unit = {
    val rootSchemaPlus: SchemaPlus = CalciteSchema.createRootSchema(true, false).plus()
    val catalog = CommonTestData.getMockedFlinkExternalCatalog
    externalCatalogSchema = ExternalCatalogSchema.create(rootSchemaPlus, schemaName, catalog)
    val typeFactory = new FlinkTypeFactory(new FlinkTypeSystem())
    calciteCatalogReader = new CalciteCatalogReader(
      CalciteSchema.from(rootSchemaPlus),
      false,
      Collections.emptyList(),
      typeFactory)
  }

  @Test
  def testGetSubSchema(): Unit = {
    val allSchemaObjectNames = calciteCatalogReader
        .getAllSchemaObjectNames(Lists.newArrayList(schemaName))
    assertTrue(allSchemaObjectNames.size() == 2)
    assertEquals(SqlMonikerType.SCHEMA, allSchemaObjectNames.get(0).getType)
    assertTrue(
      CollectionUtils.isEqualCollection(
        allSchemaObjectNames.get(0).getFullyQualifiedNames,
        Lists.newArrayList(schemaName, db)
      ))
  }

  @Test
  def testGetTable(): Unit = {
    val relOptTable = calciteCatalogReader.getTable(Lists.newArrayList(schemaName, db, tb))
    assertNotNull(relOptTable)
    val tableSourceTable = relOptTable.unwrap(classOf[TableSourceTable[_]])
    tableSourceTable match {
      case tst: TableSourceTable[_] =>
        assertTrue(tst.tableSource.isInstanceOf[CsvTableSource])
      case _ =>
        fail("unexpected table type!")
    }
  }

  @Test
  def testGetNotExistTable(): Unit = {
    val relOptTable = calciteCatalogReader.getTable(
      Lists.newArrayList(schemaName, db, "nonexist-tb"))
    assertNull(relOptTable)
  }

}
