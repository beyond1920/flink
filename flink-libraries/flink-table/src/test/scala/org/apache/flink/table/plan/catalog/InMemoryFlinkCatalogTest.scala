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

import org.apache.flink.configuration.Configuration
import org.apache.calcite.schema.Table
import org.apache.flink.table.api.{DatabaseNotExistException, TableAlreadyExistException, TableNotExistException}
import org.apache.flink.table.plan.schema.TableSourceTable
import org.apache.flink.table.utils.CommonTestData
import org.junit.{Assert, Before, Test}
import org.powermock.reflect.Whitebox
import scala.collection.JavaConverters._

class InMemoryFlinkCatalogTest {

  private val databaseName = "default"
  private var catalog: FlinkCatalog = _

  @Before
  def setUp: Unit = {
    catalog = new InMemoryFlinkCatalog(new Configuration)
  }

  @Test
  def testCreateTable(): Unit = {
    Assert.assertTrue(catalog.getTableNames(databaseName).isEmpty)
    catalog.createTable(databaseName, "t1", createTableInstance(), false)
    Assert.assertTrue(catalog.getTableNames(databaseName).asScala == Set("t1"))
  }

  @Test(expected = classOf[TableAlreadyExistException])
  def testCreateExistedTable(): Unit = {
    val tableName = "t1"
    Whitebox.setInternalState(catalog, "")
    catalog.createTable(databaseName, tableName, createTableInstance(), false)
    catalog.createTable(databaseName, tableName, createTableInstance(), false)
  }

  @Test
  def testGetTable(): Unit = {
    val table = createTableInstance()
    catalog.createTable(databaseName, "t1", table, false)
    Assert.assertEquals(catalog.getTable(databaseName, "t1"), table)
  }

  @Test(expected = classOf[DatabaseNotExistException])
  def testGetTableUnderNotExistDatabaseName(): Unit = {
    catalog.getTable("notexistedDb", "t1")
  }

  @Test(expected = classOf[TableNotExistException])
  def testGetNotExistTable(): Unit = {
    catalog.getTable(databaseName, "t1")
  }

  @Test
  def testAlterTable(): Unit = {
    val tableName = "t1"
    val table = createTableInstance()
    catalog.createTable(databaseName, tableName, table, false)
    Assert.assertEquals(catalog.getTable(databaseName, tableName), table)
    val newTable = createTableInstance()
    catalog.alterTable(databaseName, tableName, newTable)
    val currentTable = catalog.getTable(databaseName, tableName)
    // validate the table is really replaced after alter table
    Assert.assertNotEquals(table, currentTable)
    Assert.assertEquals(newTable, currentTable)
  }

  @Test(expected = classOf[TableNotExistException])
  def testAlterNotExistTable(): Unit = {
    catalog.alterTable(databaseName, "t1", createTableInstance())
  }

  @Test
  def testDropTable(): Unit = {
    val tableName = "t1"
    catalog.createTable(databaseName, tableName, createTableInstance(), false)
    Assert.assertTrue(catalog.getTableNames(databaseName).contains(tableName))
    catalog.dropTable(databaseName, tableName, false)
    Assert.assertFalse(catalog.getTableNames(databaseName).contains(tableName))
  }

  @Test(expected = classOf[TableNotExistException])
  def testDropNotExistTable(): Unit = {
    catalog.dropTable(databaseName, "t1", false)
  }

  @Test
  def testGetDatabaseNames(): Unit = {
    Assert.assertTrue(catalog.getDatabaseNames().asScala == Set("default"))
  }

  @Test
  def testGetDatabase(): Unit = {
    Assert.assertNotNull(catalog.getDatabase(databaseName))
  }

  @Test
  def testGetNotExistDatabase(): Unit = {
    Assert.assertNull(catalog.getDatabase("notexistedDb"))
  }

  @Test
  def testGetDefaultDatabase(): Unit = {
    Assert.assertEquals(catalog.getDefaultDatabase(), databaseName)
  }

  private def createTableInstance(): Table = {
    val csvTableSource = CommonTestData.getCsvTableSource
    new TableSourceTable(csvTableSource)
  }

}
