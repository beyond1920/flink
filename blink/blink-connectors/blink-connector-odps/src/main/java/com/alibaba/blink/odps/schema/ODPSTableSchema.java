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

package com.alibaba.blink.odps.schema;

import com.aliyun.odps.Column;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Maps;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * ODPS table schema information including column information and isPartition table
 */
public class ODPSTableSchema implements Serializable {
	private static final long serialVersionUID = -6327923765714170499L;

	private final List<ODPSColumn> columns;

	private final boolean isPartition;

	private final boolean isView;

	private transient Map<String, ODPSColumn> columnMap;

	public ODPSTableSchema(
			List<Column> normalColumns,
			List<Column> partitionColumns,
			boolean isView) {

		checkArgument(
				normalColumns != null && !normalColumns.isEmpty(),
				"input normal columns cannot be null or empty!");
		FluentIterable<ODPSColumn> columnList = FluentIterable.from(normalColumns)
				.transform(new Function<Column, ODPSColumn>() {
					@Override
					public ODPSColumn apply(Column column) {
						return new ODPSColumn(
								column.getName(),
								column.getType());
					}
				});
		this.isView = isView;

		boolean hasPartitionCols = partitionColumns != null && !partitionColumns.isEmpty();

		if (hasPartitionCols) {
			columnList = columnList.append(
					FluentIterable.from(partitionColumns)
							.transform(new Function<Column, ODPSColumn>() {
								@Override
								public ODPSColumn apply(Column column) {
									return new ODPSColumn(
											column.getName(),
											column.getType(),
											true);
								}
							}));
		}
		isPartition = !isView && hasPartitionCols;
		this.columns = columnList.toList();
		rebuildColumnMap();
	}

	public List<ODPSColumn> getColumns() {
		return columns;
	}

	public boolean isPartition() {
		return isPartition;
	}

	public boolean isView() {
		return isView;
	}

	public ODPSColumn getColumn(String name) {
		return columnMap.get(name);
	}

	public boolean isPartitionColumn(String name) {
		ODPSColumn column = columnMap.get(name);
		if (column != null) {
			return column.isPartition();
		} else {
			throw new IllegalArgumentException("unknown column " + name);
		}
	}

	private void readObject(ObjectInputStream inputStream) throws IOException,
			ClassNotFoundException
	{
		inputStream.defaultReadObject();
		rebuildColumnMap();
	}

	private void rebuildColumnMap() {
		this.columnMap = Maps.uniqueIndex(columns.iterator(), new Function<ODPSColumn, String>() {
			@Override
			public String apply(@Nullable ODPSColumn column) {
				return column.getName();
			}
		});
	}
}

