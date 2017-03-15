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

import java.util.Set

import org.apache.calcite.schema.Table
import org.apache.flink.configuration.Configuration

/**
  * Interface for flink catalog.
  */
abstract class FlinkCatalog(conf: Configuration) {

  /**
    * Adds table information (including schema and statistics) into external Catalog
    *
    * @param dbName
    * @param tableName
    * @param table
    * @param ignoreIfExists
    */
  def createTable(dbName: String, tableName: String, table: Table, ignoreIfExists: Boolean): Unit

  /**
    * Deletes table information (including schema and statistics) from external Catalog
    *
    * @param dbName
    * @param tableName
    * @param ignoreIfNotExists
    */
  def dropTable(dbName: String, tableName: String, ignoreIfNotExists: Boolean): Unit

  /**
    * Alters existed table information (including schema or statistics) into external Catalog
    *
    * @param dbName
    * @param tableName
    * @param table
    */
  def alterTable(dbName: String, tableName: String, table: Table): Unit

  /**
    * Adds or alters table information (including schema or statistics) into external Catalog
    *
    * @param dbName
    * @param tableName
    * @param table
    * @return the previous table associated with dbName and tableName,
    *         or null if there was no table mapping for dbName and tableName
    */
  def createOrUpdate(dbName: String, tableName: String, table: Table): Table

  /**
    * Gets table information from external Catalog, and rebuild table from these information
    *
    * @param dbName
    * @param tableName
    * @return
    */
  def getTable(dbName: String, tableName: String): Table

  /**
    * Gets the table name lists from current external Catalog
    *
    * @param dbName
    * @return
    */
  def getTableNames(dbName: String): Set[String]

  def getDatabase(dbName: String): CatalogDatabase

  /**
    * Gets the database name lists from current external Catalog
    *
    * @return
    */
  def getDatabaseNames(): Set[String]

  /**
    * Gets default database where retrieve table and add table
    *
    * @return the default database where retrieve table and add table;
    *         or null if there is no default database
    */
  def getDefaultDatabase(): String

}
