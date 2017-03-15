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

package com.alibaba.blink.odps.externalcatalog;

import com.aliyun.odps.PartitionSpec;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * The converter is used to convert PartitionSpec of {@link org.apache.flink.table.catalog.ExternalCatalogTablePartition}
 * to or from Odps {@link PartitionSpec}.
 */
public class PartitionSpecConverter {

	private PartitionSpecConverter() {

	}

	/**
	 * convert PartitionSpec of ExternalCatalogTablePartition to Odps PartitionSpec
	 *
	 * @param partSpecMap
	 * @return
	 */
	public static PartitionSpec fromMap(Map<String, String> partSpecMap) {
		PartitionSpec odpsPartitionSpec = new PartitionSpec();
		for (Map.Entry<String, String> partValue : partSpecMap.entrySet()) {
			odpsPartitionSpec.set(partValue.getKey(), partValue.getValue());
		}
		return odpsPartitionSpec;
	}

	/**
	 * convert Odps PartitionSpec to PartitionSpec of ExternalCatalogTablePartition
	 *
	 * @param partSpec
	 * @return
	 */
	public static Map<String, String> toMap(PartitionSpec partSpec) {
		ImmutableMap.Builder<String, String> partitionMapBuilder = ImmutableMap.builder();
		for (String key : partSpec.keys()) {
			partitionMapBuilder.put(key, partSpec.get(key));
		}
		return partitionMapBuilder.build();
	}

}
