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

import org.apache.calcite.jdbc.CalciteSchema
import org.apache.calcite.schema.{Schema, SchemaPlus, Table}
import org.apache.flink.configuration.Configuration

/**
  * Catalog schema implementation of Calcite Schema
  *
  * @param parentSchemaPlus parent schema plus
  * @param schemaName       current schema name
  * @param flinkCatalog     flink catalog
  */
class FlinkCatalogSchema(
    parentSchemaPlus: SchemaPlus,
    schemaName: String,
    flinkCatalog: FlinkCatalog)
    extends FlinkCatalogAbstractSchema(parentSchemaPlus, schemaName, flinkCatalog) {

  private val defaultSubSchema: Option[FlinkCatalogDatabaseSchema] =
    Option(flinkCatalog.getDefaultDatabase()).map(
      new FlinkCatalogDatabaseSchema(getLocalRootSchemaPlus, _, flinkCatalog))

  override def getSubSchema(name: String): Schema = {
    val database = flinkCatalog.getDatabase(name)
    if (database != null) {
      new FlinkCatalogDatabaseSchema(getLocalRootSchemaPlus, name, flinkCatalog)
    } else {
      null
    }
  }

  override def getSubSchemaNames: Set[String] = flinkCatalog.getDatabaseNames()

  override def getTable(name: String): Table = defaultSubSchema match {
    case Some(subSchema) => subSchema.getTable(name)
    case None => super.getTable(name)
  }

  override def getTableNames: Set[String] = defaultSubSchema match {
    case Some(subSchema) => subSchema.getTableNames
    case None => super.getTableNames
  }

  /**
    * Drops a table from default sub-schema if defined.
    *
    * @param name table name
    */
  override def dropTable(name: String): Unit = defaultSubSchema match {
    case Some(subSchema) => {
      subSchema.dropTable(name)
      CalciteSchema.from(this.localRootSchemaPlus).tableMap.remove(name)
    }
    case None => super.dropTable(name)
  }

  /**
    * Adds a table from default sub-schema if defined.
    *
    * @param name table name
    * @param table
    */
  override def add(name: String, table: Table): Option[Table] = defaultSubSchema match {
    case Some(subSchema) => {
      val replacedTable = subSchema.add(name, table)
      if(replacedTable.isDefined) {
        CalciteSchema.from(this.localRootSchemaPlus).tableMap.remove(name)
      }
      replacedTable
    }
    case None => super.add(name, table)
  }

  override def getDefaultSchema(): Schema = defaultSubSchema match {
    case Some(subSchema) => subSchema
    case None => this
  }

  private class FlinkCatalogDatabaseSchema(
      parentSchemaPlus: SchemaPlus,
      dbName: String,
      flinkCatalog: FlinkCatalog)
      extends FlinkCatalogAbstractSchema(parentSchemaPlus, dbName, flinkCatalog) {

    /**
      * Returns a table with a given name, or null if not found.
      *
      * @param name Table name
      * @return the table with the given name, or null if not found
      */
    override def getTable(name: String): Table = flinkCatalog.getTable(dbName, name)

    /**
      * Returns the names of the tables in this schema.
      *
      * @return names of the tables in this schema
      */
    override def getTableNames: Set[String] = flinkCatalog.getTableNames(dbName)

    /**
      * Drops a table from current schema.
      *
      * @param name table name
      */
    override def dropTable(name: String): Unit = {
      flinkCatalog.dropTable(dbName, name, false)
      CalciteSchema.from(this.localRootSchemaPlus).tableMap.remove(name)
    }

    /**
      * Adds a table or Updates a table to current schema.
      *
      * @param name table name
      * @param table
      * @return replaced table
      */
    override def add(name: String, table: Table): Option[Table] = {
      val replacedTable = flinkCatalog.createOrUpdate(dbName, name, table)
      if (replacedTable != null) {
        CalciteSchema.from(this.localRootSchemaPlus).tableMap.remove(name)
      }
      Option(replacedTable)
    }
  }

}


/**
  * Factory for FlinkCatalogSchema objects.
  */
object FlinkCatalogSchema {

  /**
    * Creates a FlinkCatalogSchema.
    *
    * @param parentSchema Parent schema
    * @param name         Name of this schema
    * @param catalogClazz Catalog class
    * @param catalogConf  Configuration of catalog
    * @return Created schema
    */
  def create(
      parentSchema: SchemaPlus,
      name: String,
      catalogClazz: Class[_ <: FlinkCatalog],
      catalogConf: Configuration): FlinkCatalogSchema = {
    val catalog = catalogClazz
        .getConstructor(classOf[Configuration])
        .newInstance(catalogConf)
    new FlinkCatalogSchema(parentSchema, name, catalog)
  }

}
