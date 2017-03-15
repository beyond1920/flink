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

import org.apache.flink.table.api._
import org.apache.flink.table.catalog.ExternalCatalogTypes.PartitionSpec

import _root_.scala.collection.mutable

/**
  * This class is an in-memory implementation of [[ExternalCatalog]].
  *
  * It could be used for testing or developing instead of used in production environment.
  */
class InMemoryExternalCatalog extends ExternalCatalog {

  private val databases = new mutable.HashMap[String, DatabaseDesc]

  override def createTable(
      table: ExternalCatalogTable,
      ignoreIfExists: Boolean): Unit = synchronized {
    val dbName = table.identifier.database
    val tables = getTables(dbName)
    val tableName = table.identifier.table
    if (tables.contains(tableName)) {
      if (!ignoreIfExists) {
        throw new TableAlreadyExistException(dbName, tableName)
      }
    } else {
      tables.put(tableName, new TableDesc(table))
    }
  }

  override def dropTable(
      dbName: String,
      tableName: String,
      ignoreIfNotExists: Boolean): Unit = synchronized {
    val tables = getTables(dbName)
    if (tables.remove(tableName).isEmpty && !ignoreIfNotExists) {
      throw new TableNotExistException(dbName, tableName)
    }
  }

  override def alterTable(table: ExternalCatalogTable): Unit = synchronized {
    val tableDesc = getTableDesc(table.identifier.database, table.identifier.table)
    tableDesc.table = table
  }

  override def listTables(dbName: String): Seq[String] = synchronized {
    val tables = getTables(dbName)
    tables.keys.toSeq
  }

  override def getTable(dbName: String, tableName: String): ExternalCatalogTable = synchronized {
    val tableDesc = getTableDesc(dbName, tableName)
    tableDesc.table
  }

  override def createDatabase(
      db: ExternalCatalogDatabase,
      ignoreIfExists: Boolean): Unit = synchronized {
    val dbName = db.dbName
    if (databases.contains(dbName)) {
      if (!ignoreIfExists) {
        throw new DatabaseAlreadyExistException(dbName)
      }
    } else {
      databases.put(dbName, new DatabaseDesc(db))
    }
  }

  override def alterDatabase(db: ExternalCatalogDatabase): Unit = synchronized {
    val dbName = db.dbName
    databases.get(dbName) match {
      case Some(database) => database.db = db
      case None => throw new DatabaseNotExistException(dbName)
    }
  }

  override def dropDatabase(
      dbName: String,
      ignoreIfNotExists: Boolean): Unit = synchronized {
    if (databases.remove(dbName).isEmpty && !ignoreIfNotExists) {
      throw new DatabaseNotExistException(dbName)
    }
  }

  override def listDatabases(): Seq[String] = synchronized {
    databases.keys.toSeq
  }

  override def getDatabase(dbName: String): ExternalCatalogDatabase = synchronized {
    databases.get(dbName) match {
      case Some(database) => database.db
      case None => null
    }
  }

  override def createPartition(
      dbName: String,
      tableName: String,
      part: ExternalCatalogTablePartition,
      ignoreIfExists: Boolean): Unit = synchronized {
    val newPartSpec = part.partitionSpec
    val partitionedTable = getPartitionedTable(dbName, tableName)
    checkPartitionSpec(newPartSpec, partitionedTable.table)
    if (partitionedTable.partitions.contains(newPartSpec)) {
      if (!ignoreIfExists) {
        throw new PartitionAlreadyExistException(dbName, tableName, newPartSpec)
      }
    } else {
      partitionedTable.partitions.put(newPartSpec, part)
    }
  }

  override def getPartition(
      dbName: String,
      tableName: String,
      partSpec: PartitionSpec): ExternalCatalogTablePartition = synchronized {
    val partitionedTable = getPartitionedTable(dbName, tableName)
    checkPartitionSpec(partSpec, partitionedTable.table)
    partitionedTable.partitions.get(partSpec) match {
      case Some(part) => part
      case None =>
        throw new PartitionNotExistException(dbName, tableName, partSpec)
    }
  }

  override def dropPartition(
      dbName: String,
      tableName: String,
      partSpec: PartitionSpec,
      ignoreIfNotExists: Boolean): Unit = synchronized {
    val partitionedTable = getPartitionedTable(dbName, tableName)
    checkPartitionSpec(partSpec, partitionedTable.table)
    if (partitionedTable.partitions.remove(partSpec).isEmpty && !ignoreIfNotExists) {
      throw new PartitionNotExistException(dbName, tableName, partSpec)
    }
  }

  override def listPartitionSpec(
      dbName: String,
      tableName: String): Seq[PartitionSpec] = synchronized {
    getPartitionedTable(dbName, tableName).partitions.keys.toSeq
  }

  override def alterPartition(
      dbName: String,
      tableName: String,
      part: ExternalCatalogTablePartition): Unit = synchronized {
    val updatedPartSpec = part.partitionSpec
    val partitionedTable = getPartitionedTable(dbName, tableName)
    checkPartitionSpec(updatedPartSpec, partitionedTable.table)
    if (partitionedTable.partitions.contains(updatedPartSpec)) {
      partitionedTable.partitions.put(updatedPartSpec, part)
    } else {
      throw new PartitionNotExistException(dbName, tableName, updatedPartSpec)
    }
  }

  private def getTables(db: String): mutable.HashMap[String, TableDesc] =
    databases.get(db) match {
      case Some(database) => database.tables
      case None => throw new DatabaseNotExistException(db)
    }

  private def getTableDesc(
      dbName: String,
      tableName: String): TableDesc = {
    val tables = getTables(dbName)
    tables.get(tableName) match {
      case Some(tableDesc) => tableDesc
      case None =>
        throw new TableNotExistException(dbName, tableName)
    }
  }

  private def getPartitionedTable(
      dbName: String,
      tableName: String): TableDesc = {
    val tableDesc = getTableDesc(dbName, tableName)
    val table = tableDesc.table
    if (table.isPartitioned) {
      tableDesc
    } else {
      throw new UnsupportedOperationException(
        s"cannot do any operation about partition on the non-partitioned table ${table.identifier}")
    }
  }

  private def checkPartitionSpec(partSpec: PartitionSpec, table: ExternalCatalogTable): Unit =
    if (partSpec.keySet != table.partitionColumnNames.toSet) {
      throw new IllegalArgumentException("Input partition specification is invalid!")
    }

  private class DatabaseDesc(var db: ExternalCatalogDatabase) {
    val tables = new mutable.HashMap[String, TableDesc]
  }

  private class TableDesc(var table: ExternalCatalogTable) {
    val partitions = new mutable.HashMap[PartitionSpec, ExternalCatalogTablePartition]
  }

}
