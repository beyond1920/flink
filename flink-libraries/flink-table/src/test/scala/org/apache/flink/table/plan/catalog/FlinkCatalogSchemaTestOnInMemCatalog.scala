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

package org.apache.flink.table.plan.catalog

import java.util.Collections

import com.google.common.collect.Lists
import org.apache.calcite.jdbc.CalciteSchema
import org.apache.calcite.prepare.CalciteCatalogReader
import org.apache.calcite.schema.{SchemaPlus, Table}
import org.apache.calcite.sql.validate.{SqlMoniker, SqlMonikerType}
import org.apache.calcite.tools.Frameworks
import org.apache.commons.collections.CollectionUtils
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.calcite.{FlinkTypeFactory, FlinkTypeSystem}
import org.apache.flink.table.plan.schema.{FlinkTable, TableSourceTable}
import org.apache.flink.table.utils.CommonTestData
import org.junit.{Assert, Before, Test}

import scala.collection.JavaConverters._

class FlinkCatalogSchemaTestOnInMemCatalog {

  private val schemaName = "test"
  private var flinkCatalogSchema: FlinkCatalogSchema = _
  private var calciteCatalogReader: CalciteCatalogReader = _

  @Before
  def setUp: Unit = {
    val rootSchemaPlus: SchemaPlus = Frameworks.createRootSchema(true)
    val catalog = new InMemoryFlinkCatalog(new Configuration)
    flinkCatalogSchema = new FlinkCatalogSchema(rootSchemaPlus, schemaName, catalog)
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
    Assert.assertTrue(allSchemaObjectNames.size() == 1)
    Assert.assertEquals(SqlMonikerType.SCHEMA, allSchemaObjectNames.get(0).getType)
    Assert.assertTrue(
      CollectionUtils.isEqualCollection(
        allSchemaObjectNames.get(0).getFullyQualifiedNames,
        Lists.newArrayList(schemaName, "default")
      ))
  }

  @Test
  def testAddTable(): Unit = {
    Assert.assertNull(calciteCatalogReader.getTable(Lists.newArrayList(schemaName, "t1")))
    flinkCatalogSchema.add("t1", createTableInstance())
    Assert.assertNotNull(calciteCatalogReader.getTable(Lists.newArrayList(schemaName, "t1")))
  }

  @Test
  def testGetTable(): Unit = {
    val table = createTableInstance()
    flinkCatalogSchema.add("t1", table)
    val wrappedTable = calciteCatalogReader.getTable(Lists.newArrayList(schemaName, "t1"))
    wrappedTable.unwrap(classOf[FlinkTable[_]]) match {
      case t: FlinkTable[_] =>
        Assert.assertEquals(table, t)
      case _ =>
        Assert.fail("table is not expected")
    }
  }

  @Test
  def testGetTableNames(): Unit = {
    Assert.assertEquals(getTableObjectNames.length, 0)

    flinkCatalogSchema.add("t1", createTableInstance())
    flinkCatalogSchema.add("t2", createTableInstance())
    val tableNames = getTableObjectNames
    Assert.assertEquals(tableNames.length, 2)
    Assert.assertEquals(
      Lists.newArrayList(schemaName, "t1"),
      tableNames(0).getFullyQualifiedNames)
    Assert.assertEquals(
      Lists.newArrayList(schemaName, "t2"),
      tableNames(1).getFullyQualifiedNames)
  }

  @Test
  def testDropTable(): Unit = {
    flinkCatalogSchema.add("t1", createTableInstance())
    Assert.assertNotNull(calciteCatalogReader.getTable(Lists.newArrayList(schemaName, "t1")))
    flinkCatalogSchema.dropTable("t1")
    Assert.assertNull(calciteCatalogReader.getTable(Lists.newArrayList(schemaName, "t1")))
  }

  @Test
  def testAlterTable(): Unit = {
    val originTable = createTableInstance()
    flinkCatalogSchema.add("t1", originTable)
    calciteCatalogReader.getTable(Lists.newArrayList(schemaName, "t1"))
        .unwrap(classOf[FlinkTable[_]]) match {
      case t: FlinkTable[_] =>
        Assert.assertEquals(originTable, t)
      case _ =>
        Assert.fail("table is not expected")
    }
    val newTable = createTableInstance()
    flinkCatalogSchema.add("t1", newTable)
    calciteCatalogReader.getTable(Lists.newArrayList(schemaName, "t1"))
        .unwrap(classOf[FlinkTable[_]]) match {
      case t: FlinkTable[_] =>
        Assert.assertNotEquals(originTable, t)
        Assert.assertEquals(newTable, t)
      case _ =>
        Assert.fail("table is not expected")
    }
  }

  private def createTableInstance(): Table = {
    val csvTableSource = CommonTestData.getCsvTableSource
    new TableSourceTable(csvTableSource)
  }

  private def getTableObjectNames: Seq[SqlMoniker] = {
    calciteCatalogReader
        .getAllSchemaObjectNames(Lists.newArrayList(schemaName))
        .asScala
        .filter(_.getType == SqlMonikerType.TABLE)
  }
}
