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

/**
  * This class is responsible for interact with external catalog.
  * Its main responsibilities including:
  * <ul>
  * <li> create/drop/alter database or tables for DDL operations
  * <li> provide tables for calcite catalog, it looks up databases or tables in the external catalog
  * </ul>
  */
trait ExternalCatalog {

  /**
    * Adds table into external Catalog
    *
    * @param table          description of table which to create
    * @param ignoreIfExists whether to ignore operation if table already exists
    */
  def createTable(table: ExternalCatalogTable, ignoreIfExists: Boolean): Unit

  /**
    * Deletes table from external Catalog
    *
    * @param dbName            database name
    * @param tableName         table name
    * @param ignoreIfNotExists whether to ignore operation if table not exist yet
    */
  def dropTable(dbName: String, tableName: String, ignoreIfNotExists: Boolean): Unit

  /**
    * Alters existed table into external Catalog
    *
    * @param table description of table which to alter
    */
  def alterTable(table: ExternalCatalogTable): Unit

  /**
    * Gets table from external Catalog
    *
    * @param dbName    database name
    * @param tableName table name
    * @return table
    */
  def getTable(dbName: String, tableName: String): ExternalCatalogTable

  /**
    * Gets the table name lists from current external Catalog
    *
    * @param dbName database name
    * @return lists of table name
    */
  def listTables(dbName: String): Seq[String]

  /**
    * Adds database into external Catalog
    *
    * @param db             database name
    * @param ignoreIfExists whether to ignore operation if database already exists
    */
  def createDatabase(db: ExternalCatalogDatabase, ignoreIfExists: Boolean): Unit

  /**
    * Deletes database from external Catalog
    *
    * @param dbName            database name
    * @param ignoreIfNotExists whether to ignore operation if table not exist yet
    */
  def dropDatabase(dbName: String, ignoreIfNotExists: Boolean): Unit

  /**
    * Alters existed database into external Catalog
    */
  def alterDatabase(db: ExternalCatalogDatabase): Unit

  /**
    * Gets database from external Catalog
    *
    * @param dbName database name
    * @return database
    */
  def getDatabase(dbName: String): ExternalCatalogDatabase

  /**
    * Gets the database name lists from current external Catalog
    *
    * @return
    */
  def listDatabases(): Seq[String]

}
