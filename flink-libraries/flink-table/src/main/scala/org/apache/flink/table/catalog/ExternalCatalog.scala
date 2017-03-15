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

import org.apache.flink.table.catalog.ExternalCatalogTypes.PartitionSpec

/**
  * This class is responsible for interact with external catalog.
  * Its main responsibilities including:
  * <ul>
  * <li> create/drop/alter database, tables, partitions for DDL operations
  * <li> provide tables for calcite catalog, it looks up databases or tables in the external catalog
  * </ul>
  */
trait ExternalCatalog {

  /**
    * Adds partitions into an external Catalog table
    *
    * @param dbName         database name
    * @param tableName      table name
    * @param part           partition description of partition which to create
    * @param ignoreIfExists whether to ignore operation if table already exists
    */
  def createPartition(
      dbName: String,
      tableName: String,
      part: ExternalCatalogTablePartition,
      ignoreIfExists: Boolean): Unit

  /**
    * Deletes partition of an external Catalog table
    *
    * @param dbName            database name
    * @param tableName         table name
    * @param partSpec          partition specification
    * @param ignoreIfNotExists whether to ignore operation if table not exist yet
    */
  def dropPartition(
      dbName: String,
      tableName: String,
      partSpec: PartitionSpec,
      ignoreIfNotExists: Boolean): Unit

  /**
    * Alters an existed external Catalog table partition
    *
    * @param dbName    database name
    * @param tableName table name
    * @param part      description of partition which to alter
    */
  def alterPartition(dbName: String, tableName: String, part: ExternalCatalogTablePartition): Unit

  /**
    * Gets the partition from external Catalog
    *
    * @param dbName    database name
    * @param tableName table name
    * @param partSpec  partition specification
    * @return
    */
  def getPartition(
      dbName: String,
      tableName: String,
      partSpec: PartitionSpec): ExternalCatalogTablePartition

  /**
    * Gets the partition specification list of a table from external catalog
    *
    * @param dbName    database name
    * @param tableName table name
    * @return
    */
  def listPartitionSpec(dbName: String, tableName: String): Seq[PartitionSpec]

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
