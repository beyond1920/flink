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

import java.util.{Collection, Collections, Set}

import org.apache.calcite.linq4j.tree.{DefaultExpression, Expression}
import org.apache.calcite.schema.{Function, Schema, SchemaPlus, Table}

import scala.collection.JavaConverters._

abstract class FlinkCatalogAbstractSchema(
    parentSchemaPlus: SchemaPlus,
    schemaName: String,
    flinkCatalog: FlinkCatalog) extends Schema {

  // add current schema to parent schemaPlus as a sub-schema,
  // returned the wrapped schema which is local root schema
  protected val localRootSchemaPlus = parentSchemaPlus.add(schemaName, this)

  registerSubSchemaToLocalRoot()

  private val expression: Expression = new DefaultExpression(classOf[Any])

  override def getSubSchema(name: String): Schema = null

  override def getSubSchemaNames: Set[String] = Collections.emptySet[String]

  override def getTable(name: String): Table = null

  override def getTableNames: Set[String] = Collections.emptySet[String]

  override def getFunctions(name: String): Collection[Function] =
    Collections.emptyList[Function]

  override def getFunctionNames: Set[String] = Collections.emptySet[String]

  override def isMutable: Boolean = true

  override def getExpression(parentSchema: SchemaPlus, name: String): Expression = expression

  override def contentsHaveChangedSince(lastCheck: Long, now: Long): Boolean = true

  /**
    * Adds a table to current schema.
    *
    * @param name table name
    * @param table
    */
  def add(name: String, table: Table): Option[Table] =
    throw new UnsupportedOperationException(
      s"Add table is not supported in schema $schemaName")

  /**
    * Drops a table from current schema.
    *
    * @param name table name
    */
  def dropTable(name: String): Unit =
    throw new UnsupportedOperationException(
      s"Drop table is not supported in schema $schemaName")

  /**
    * The schema can be a top level schema which doesn't have its own tables, but delivers
    * to the default sub schemas for table look up. Default implementation returns itself.
    *
    * @return default schema where tables are created or retrieved from.
    */
  def getDefaultSchema(): Schema = this

  /**
    * Gets local root schema.
    *
    * @return
    */
  final def getLocalRootSchemaPlus: SchemaPlus = localRootSchemaPlus

  /**
    * Adds all sub-schema of current schema to local root schema as sub-schemas.
    */
  private def registerSubSchemaToLocalRoot(): Unit =
    getSubSchemaNames.asScala.foreach(getSubSchema(_))
}
