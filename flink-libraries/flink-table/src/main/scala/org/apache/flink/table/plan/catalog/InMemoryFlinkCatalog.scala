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

import java.util.concurrent.ConcurrentHashMap
import java.util.{Set, Map}

import com.google.common.collect.ImmutableMap
import org.apache.calcite.schema.Table
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.api.{TableAlreadyExistException, DatabaseNotExistException, TableNotExistException}

/**
  * An in-memory implementation of the table catalog.
  *
  */
class InMemoryFlinkCatalog(conf: Configuration) extends FlinkCatalog(conf) {

  private val defaultDatabase = "default"

  // the map from database name to CatalogDatabase
  private val databases: Map[String, CatalogDatabase] =
    ImmutableMap.of(defaultDatabase, CatalogDatabase(defaultDatabase))

  // the map from database names to its underlying tables
  private val database2Table: Map[String, ConcurrentHashMap[String, Table]] =
    ImmutableMap.of(defaultDatabase, new ConcurrentHashMap[String, Table]())

  override def createTable(
      dbName: String,
      tableName: String,
      table: Table,
      ignoreIfExists: Boolean): Unit = {
    val tables = getTables(dbName)
    val oldTable = tables.putIfAbsent(tableName, table)
    if (oldTable != null && !ignoreIfExists) {
      throw new TableAlreadyExistException(tableName)
    }
  }

  override def dropTable(dbName: String, tableName: String, ignoreIfNotExists: Boolean): Unit = {
    val tables = getTables(dbName)
    val deletedTable = tables.remove(tableName)
    if (deletedTable == null && !ignoreIfNotExists) {
      throw new TableNotExistException(tableName)
    }
  }

  override def alterTable(dbName: String, tableName: String, table: Table): Unit = {
    val tables = getTables(dbName)
    val replacedTable = tables.replace(tableName, table)
    if (replacedTable == null) {
      throw new TableNotExistException(tableName)
    }
  }

  override def createOrUpdate(dbName: String, tableName: String, table: Table): Table = {
    val tables = getTables(dbName)
    tables.put(tableName, table)
  }

  override def getTable(dbName: String, tableName: String): Table = {
    val tables = getTables(dbName)
    val table = tables.get(tableName)
    if (table == null) {
      throw new TableNotExistException(tableName)
    }
    table
  }

  override def getTableNames(dbName: String): Set[String] = {
    val tables = getTables(dbName)
    tables.keySet()
  }

  override def getDatabaseNames(): Set[String] = databases.keySet

  override def getDatabase(dbName: String): CatalogDatabase = {
    databases.get(dbName)
  }

  /**
    * Gets default database where retrieve table and add table
    *
    * @return
    */
  override def getDefaultDatabase(): String = defaultDatabase

  /**
    * Gets all tables underlying a database
    *
    * @param dbName database name
    * @return all tables underlying a database
    */
  private def getTables(dbName: String): ConcurrentHashMap[String, Table] = {
    val tables = database2Table.get(dbName)
    if (tables == null) {
      throw new DatabaseNotExistException(dbName)
    }
    tables
  }
}
