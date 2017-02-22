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

import java.io.IOException
import java.lang.reflect.Modifier
import java.net.URL
import java.util.Properties

import org.apache.flink.table.annotation.ExternalCatalogCompatible
import org.apache.flink.table.api.{ExternalCatalogTableTypeAlreadyExistException, ExternalCatalogTableTypeNotExistException}
import org.apache.flink.table.plan.schema.TableSourceTable
import org.apache.flink.table.plan.stats.FlinkStatistic
import org.apache.flink.table.sources.TableSource
import org.apache.flink.util.InstantiationUtil
import org.reflections.Reflections
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap

/**
  * The utility class is used to convert ExternalCatalogTable to TableSourceTable.
  */
object CatalogTableHelper {

  // config file to specifier the scan package to search tableSources
  // which is compatible with external catalog.
  private val tableSourceConfigFileName = "externalcatalogTable.properties"

  private val LOG: Logger = LoggerFactory.getLogger(this.getClass)

  // registered tableSources which are compatible with external catalog.
  // Key is table type name, Value is converter class.
  private val tableSourceTypeToConvertersClazz = {
    val registeredConverters =
      new HashMap[String, Class[_ <: TableSourceConverter[_]]]
    // scan all config file to find all tableSources which are compatible with external catalog.
    val resourceUrls = getClass.getClassLoader.getResources(tableSourceConfigFileName)
    while (resourceUrls.hasMoreElements) {
      val url = resourceUrls.nextElement()
      parseScanPackageFromConfigFile(url) match {
        case Some(scanPackage) =>
          val clazzWithAnnotations = new Reflections(scanPackage)
              .getTypesAnnotatedWith(classOf[ExternalCatalogCompatible])
          clazzWithAnnotations.asScala.foreach(clazz =>
            if (classOf[TableSource[_]].isAssignableFrom(clazz)) {
              if (Modifier.isAbstract(clazz.getModifiers()) ||
                  Modifier.isInterface(clazz.getModifiers)) {
                LOG.warn(
                  s"class :[${clazz.getName}] is also with ExternalCatalogCompatible annotation, " +
                      s"but it's an abstract clazz or an interface.")
              } else {
                val externalCatalogCompatible: ExternalCatalogCompatible =
                  clazz.getAnnotation(classOf[ExternalCatalogCompatible])
                val tableSourceName = externalCatalogCompatible.tableType()
                if (registeredConverters.contains(tableSourceName)) {
                  LOG.error(s"The table type [$tableSourceName] is already registered.")
                  throw new ExternalCatalogTableTypeAlreadyExistException(tableSourceName)
                }
                registeredConverters.put(tableSourceName, externalCatalogCompatible.converter())
              }
            } else {
              LOG.warn(
                s"class :[${clazz.getName}] is also with ExternalCatalogCompatible annotation, " +
                    s"but it's not sub-class of tableSource.")
            }
          )
        case None =>
      }
    }
    registeredConverters
  }

  /**
    * Converts an [[ExternalCatalogTable]] instance to a [[TableSourceTable]] instance
    *
    * @param externalCatalogTable the [[ExternalCatalogTable]] instance which to convert
    * @return converted [[TableSourceTable]] instance from the input catalog table
    */
  def fromExternalCatalogTable(externalCatalogTable: ExternalCatalogTable): TableSourceTable[_] = {
    val tableType = externalCatalogTable.tableType
    tableSourceTypeToConvertersClazz.get(tableType) match {
      case Some(converterClazz) =>
        val converter = InstantiationUtil.instantiate(converterClazz)
        val convertedTableSource: TableSource[_] =
          converter.fromExternalCatalogTable(externalCatalogTable) match {
            case ts: TableSource[_] => ts
            case _ => throw new RuntimeException(
              s"the converted result from converter ${converterClazz.getName} " +
                  s"is not tableSource instance")
          }

        val flinkStatistic = externalCatalogTable.stats match {
          case Some(stats) => FlinkStatistic.of(stats)
          case None => FlinkStatistic.UNKNOWN
        }
        new TableSourceTable(convertedTableSource, flinkStatistic)
      case None =>
        LOG.error(s"the table type [$tableType] does not exist.")
        throw new ExternalCatalogTableTypeNotExistException(tableType)
    }
  }

  private def parseScanPackageFromConfigFile(url: URL): Option[String] = {
    val properties = new Properties()
    try {
      properties.load(url.openStream())
      val scanPackage = properties.getProperty("scan.package")
      if (scanPackage == null || scanPackage.isEmpty) {
        LOG.warn(s"config file $url does not contain scan package!")
        None
      } else {
        Some(scanPackage)
      }
    } catch {
      case e: IOException =>
        LOG.warn(s"fail to open config file [$url]", e)
        None
    }
  }

}
