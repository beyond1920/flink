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

package com.alibaba.blink.odps;

import org.apache.flink.table.catalog.ExternalCatalogTable;
import org.apache.flink.table.catalog.TableSourceConverter;

import scala.collection.immutable.Map;

/**
 * The converter is used to convert {@link ODPSTableSource} to or from {@link ExternalCatalogTable}.
 */
public class ODPSTableSourceConverter implements TableSourceConverter<ODPSTableSource> {

	@Override
	public ODPSTableSource fromExternalCatalogTable(ExternalCatalogTable externalCatalogTable) {
		Map<String, String> properties = externalCatalogTable.properties();
		String accessId = properties.getOrElse("accessId", null);
		String accessKey = properties.getOrElse("accessKey", null);
		String endpoint = properties.getOrElse("endpoint", null);
		String project = externalCatalogTable.identifier().database();
		String table = externalCatalogTable.identifier().table();
		ODPSTableSource odpsTableSource = ODPSTableSource.builder()
				.setAccessId(accessId)
				.setAccessKey(accessKey)
				.setEndpoint(endpoint)
				.setProject(project)
				.setTable(table)
				.build();
		return odpsTableSource;
	}

}
