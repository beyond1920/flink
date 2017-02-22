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

package org.apache.flink.table.annotation;

import org.apache.flink.annotation.Public;
import org.apache.flink.table.catalog.TableSourceConverter;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * A tableSource with this annotation represents it is compatible with external catalog, that is,
 * an instance of this tableSource can be converted to or converted from external catalog table
 * instance.
 * The annotation contains the following information:
 * <ul>
 * <li> external catalog table type name for this kind of tableSource </li>
 * <li> external catalog table <-> tableSource converter class </li>
 * </ul>
 */
@Documented
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Public
public @interface ExternalCatalogCompatible {

	/**
	 * Specifies the external catalog table type of this kind of tableSource.
	 * e.g tableType of CsvTableSource can be csv, the tableType of HBaseTableSource can be hbase.
	 *
	 * @return the external catalog table type of this kind of tableSource.
	 */
	String tableType();

	/**
	 * Specifies the converter class for this kind of tableSource
	 * which is used to convert {@link org.apache.flink.table.catalog.ExternalCatalogTable} to
	 * or from {@link org.apache.flink.table.sources.TableSource}
	 *
	 * @return the converter class for this kind of tableSource.
	 */
	Class<? extends TableSourceConverter<?>> converter();

}
