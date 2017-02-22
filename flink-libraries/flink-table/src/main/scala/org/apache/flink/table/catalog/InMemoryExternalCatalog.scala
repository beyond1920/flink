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

import org.apache.flink.table.api.{DatabaseAlreadyExistException, DatabaseNotExistException, TableAlreadyExistException, TableNotExistException}

import scala.collection.mutable.HashMap

/**
  * This class is an in-memory implementation of [[ExternalCatalog]].
  *
  * It could be used for testing or developing instead of used in production environment.
  */
class InMemoryExternalCatalog extends ExternalCatalog {

  private val databases = new HashMap[String, Database]

  override def createTable(
      table: ExternalCatalogTable,
      ignoreIfExists: Boolean): Unit = synchronized {
    val dbName = table.identifier.database
    val tables = getTables(dbName)
    val tableName = table.identifier.table
    if (tables.contains(tableName) && !ignoreIfExists) {
      throw new TableAlreadyExistException(dbName, tableName)
    } else {
      tables.put(tableName, table)
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
    val dbName = table.identifier.database
    val tables = getTables(dbName)
    val tableName = table.identifier.table
    if (tables.contains(tableName)) {
      tables.put(tableName, table)
    } else {
      throw new TableNotExistException(dbName, tableName)
    }
  }

  override def listTables(dbName: String): Seq[String] = synchronized {
    val tables = getTables(dbName)
    tables.keys.toSeq
  }

  override def getTable(dbName: String, tableName: String): ExternalCatalogTable = synchronized {
    val tables = getTables(dbName)
    tables.get(tableName) match {
      case Some(table) => table
      case None => throw new TableNotExistException(dbName, tableName)
    }
  }

  override def createDatabase(
      db: ExternalCatalogDatabase,
      ignoreIfExists: Boolean): Unit = synchronized {
    val dbName = db.dbName
    if (databases.contains(dbName) && !ignoreIfExists) {
      throw new DatabaseAlreadyExistException(dbName)
    } else {
      databases.put(dbName, new Database(db))
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

  private def getTables(db: String): HashMap[String, ExternalCatalogTable] =
    databases.get(db) match {
      case Some(database) => database.tables
      case None => throw new DatabaseNotExistException(db)
    }

  private class Database(var db: ExternalCatalogDatabase) {
    val tables = new HashMap[String, ExternalCatalogTable]
  }

}
