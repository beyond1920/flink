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

package org.apache.flink.table.api

import org.apache.flink.table.catalog.ExternalCatalogTypes.PartitionSpec

/**
  * Exception for all errors occurring during expression parsing.
  */
case class ExpressionParserException(msg: String) extends RuntimeException(msg)

/**
  * Exception for all errors occurring during sql parsing.
  */
case class SqlParserException(
    msg: String,
    cause: Throwable)
  extends RuntimeException(msg, cause) {

  def this(msg: String) = this(msg, null)

}

/**
  * General Exception for all errors during table handling.
  */
case class TableException(
    msg: String,
    cause: Throwable)
  extends RuntimeException(msg, cause) {

  def this(msg: String) = this(msg, null)

}

object TableException {
  def apply(msg: String): TableException = new TableException(msg)
}

/**
  * Exception for all errors occurring during validation phase.
  */
case class ValidationException(
    msg: String,
    cause: Throwable)
  extends RuntimeException(msg, cause) {

  def this(msg: String) = this(msg, null)

}

object ValidationException {
  def apply(msg: String): ValidationException = new ValidationException(msg)
}

/**
  * Exception for unwanted method calling on unresolved expression.
  */
case class UnresolvedException(msg: String) extends RuntimeException(msg)

/**
  * Exception for operation on a nonexistent partition
  *
  * @param db            database name
  * @param table         table name
  * @param partitionSpec partition spec
  * @param cause
  */
case class PartitionNotExistException(
    db: String,
    table: String,
    partitionSpec: PartitionSpec,
    cause: Throwable)
    extends RuntimeException(
      s"partition [${partitionSpec.mkString(", ")}] does not exist in table $db.$table!", cause) {

  def this(db: String, table: String, partitionSpec: PartitionSpec) =
    this(db, table, partitionSpec, null)

}

/**
  * Exception for adding an already existed partition
  *
  * @param db            database name
  * @param table         table name
  * @param partitionSpec partition spec
  * @param cause
  */
case class PartitionAlreadyExistException(
    db: String,
    table: String,
    partitionSpec: PartitionSpec,
    cause: Throwable)
    extends RuntimeException(
      s"partition [${partitionSpec.mkString(", ")}] already exists in table $db.$table!", cause) {

  def this(db: String, table: String, partitionSpec: PartitionSpec) =
    this(db, table, partitionSpec, null)

}

/**
  * Exception for operation on a nonexistent table
  *
  * @param db    database name
  * @param table table name
  * @param cause
  */
case class TableNotExistException(
    db: String,
    table: String,
    cause: Throwable)
    extends RuntimeException(s"table $db.$table does not exist!", cause) {

  def this(db: String, table: String) = this(db, table, null)

}

/**
  * Exception for adding an already existed table
  *
  * @param db    database name
  * @param table table name
  * @param cause
  */
case class TableAlreadyExistException(
    db: String,
    table: String,
    cause: Throwable)
    extends RuntimeException(s"table $db.$table already exists!", cause) {

  def this(db: String, table: String) = this(db, table, null)

}

/**
  * Exception for operation on a nonexistent database
  *
  * @param db database name
  * @param cause
  */
case class DatabaseNotExistException(
    db: String,
    cause: Throwable)
    extends RuntimeException(s"database $db does not exist!", cause) {

  def this(db: String) = this(db, null)
}

/**
  * Exception for adding an already existed database
  *
  * @param db database name
  * @param cause
  */
case class DatabaseAlreadyExistException(
    db: String,
    cause: Throwable)
    extends RuntimeException(s"database $db already exists!", cause) {

  def this(db: String) = this(db, null)
}


/**
  * Exception for operation on a nonexistent external catalog
  *
  * @param catalogName external catalog name
  * @param cause
  */
case class ExternalCatalogNotExistException(
    catalogName: String,
    cause: Throwable)
    extends RuntimeException(s"external catalog $catalogName does not exist!", cause) {

  def this(catalogName: String) = this(catalogName, null)
}

/**
  * Exception for adding an already existed external catalog
  *
  * @param catalogName external catalog name
  * @param cause
  */
case class ExternalCatalogAlreadyExistException(
    catalogName: String,
    cause: Throwable)
    extends RuntimeException(s"external catalog $catalogName already exists!", cause) {

  def this(catalogName: String) = this(catalogName, null)
}

/**
  * Exception for operation on a nonexistent external catalog table type
  *
  * @param tableType external catalog table type
  * @param cause
  */
case class ExternalCatalogTableTypeNotExistException(
    tableType: String,
    cause: Throwable)
    extends RuntimeException(s"external catalog table type $tableType does not exist!", cause) {

  def this(tableType: String) = this(tableType, null)
}

/**
  * Exception for adding an already existed external catalog
  *
  * @param tableType external catalog table type
  * @param cause
  */
case class ExternalCatalogTableTypeAlreadyExistException(
    tableType: String,
    cause: Throwable)
    extends RuntimeException(s"external catalog table type $tableType already exists!", cause) {

  def this(tableType: String) = this(tableType, null)
}
