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

import com.alibaba.blink.odps.conf.ODPSConf;
import com.alibaba.blink.odps.inputformat.ODPSInputFormat;
import com.alibaba.blink.odps.inputformat.ODPSInputFormatBase;
import com.alibaba.blink.odps.schema.ODPSColumn;
import com.alibaba.blink.odps.schema.ODPSTableSchema;
import com.alibaba.blink.odps.type.ODPSType;
import com.alibaba.blink.odps.util.ODPSUtil;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.annotation.ExternalCatalogCompatible;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.table.sources.ProjectableTableSource;
import org.apache.flink.types.Row;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A [[BatchTableSource]] for ODPS table.
 *
 * Example :
 * ODPSTableSource odpsTable = ODPSTableSource
 * .builder()
 * .setAccessId("accessId")
 * .setAccessKey("accessKey")
 * .setEndpoint("endpoint")
 * .setProject("project")
 * .setTable("table")
 * .setPartitions(new String[]{"pt='1',ds='2'", "pt='1',ds='3'"})
 * .build();
 *
 * Note: view is not supported yet
 */
@ExternalCatalogCompatible(tableType = "odps", converter = ODPSTableSourceConverter.class)
public class ODPSTableSource implements BatchTableSource<Row>, ProjectableTableSource<Row> {

	/** odps config */
	private final ODPSConf odpsConf;

	/** identify which table to fetch */
	private final String table;

	/** identify which partition to fetch */
	private final String[] partitions;

	/** odps table schema information */
	private final ODPSTableSchema tableSchema;

	/** return fields name */
	private final String[] returnFieldsName;

	/** return fields type */
	private final TypeInformation<?>[] returnFieldsType;

	/**
	 * construct a new ODPSTableSource instance using odps config, table name,
	 * selectedFieldNames(for project pushdown), partition(for partition pushdown)
	 *
	 * @param odpsConf   odps configuration
	 * @param table      odps table name
	 * @param partitions selected partitions(for partition pushdown). if this value is null or
	 *                   blank, select all partitions of the table
	 */
	private ODPSTableSource(ODPSConf odpsConf, String table, String[] partitions) {
		this.odpsConf = checkNotNull(odpsConf);
		checkArgument(StringUtils.isNotBlank(table), "table is whitespace or null! ");
		this.table = table;
		this.tableSchema = ODPSUtil.getODPSTableSchema(odpsConf, table);
		checkArgument(!tableSchema.isView(), "view is not supported yet! ");
		if(tableSchema.isPartition() && ArrayUtils.isEmpty(partitions)) {
			this.partitions = ODPSUtil.getAllPartitions(odpsConf, table);
		} else {
			this.partitions = partitions;
		}
		int columnsNum = tableSchema.getColumns().size();
		this.returnFieldsName = new String[columnsNum];
		this.returnFieldsType = new TypeInformation<?>[columnsNum];
		for (int idx = 0; idx < columnsNum; idx++) {
			ODPSColumn column = tableSchema.getColumns().get(idx);
			returnFieldsName[idx] = column.getName();
			returnFieldsType[idx] = ODPSType.valueOf(column.getType().name()).toFlinkType();
		}
	}

	private ODPSTableSource(ODPSTableSource odpsTableSource, int[] projectFields) {
		this.odpsConf = odpsTableSource.odpsConf;
		this.table = odpsTableSource.table;
		this.partitions = odpsTableSource.partitions;
		this.tableSchema = odpsTableSource.tableSchema;
		int projectFieldsNum = projectFields.length;
		this.returnFieldsName = new String[projectFieldsNum];
		this.returnFieldsType = new TypeInformation<?>[projectFieldsNum];
		for (int idx = 0; idx < projectFieldsNum; idx++) {
			int fieldLocation = projectFields[idx];
			returnFieldsName[idx] = odpsTableSource.returnFieldsName[fieldLocation];
			returnFieldsType[idx] = odpsTableSource.returnFieldsType[fieldLocation];
		}
	}

	@Override
	public DataSet<Row> getDataSet(ExecutionEnvironment execEnv) {
		return execEnv.createInput(createODPSInputFormat(), getReturnType());
	}

	@Override
	public TypeInformation<Row> getReturnType() {
		return new RowTypeInfo(returnFieldsType, returnFieldsName);
	}

	@Override
	public ProjectableTableSource<Row> projectFields(int[] fields) {
		checkArgument(fields != null &&
				fields.length != 0 &&
				fields.length <= returnFieldsName.length,
				"project fields cannot be null or empty, " +
						"project fields number cannot be more than origin fields length!");
		return new ODPSTableSource(this, fields);
	}

	public static ODPSTableSource.Builder builder() {
		return new ODPSTableSource.Builder();
	}

	private ODPSInputFormatBase createODPSInputFormat() {
		return new ODPSInputFormat(odpsConf, table, returnFieldsName, tableSchema, partitions);
	}

	public static class Builder {
		/** ODPS configure */
		private String accessId;
		private String accessKey;
		private String endpoint;
		private String project;

		/** identify which table to fetch */
		private String table;

		/** identify which partition to fetch */
		private String[] partitions;

		public Builder() {

		}

		public Builder setAccessId(String accessId) {
			checkArgument(StringUtils.isNotBlank(accessId), "accessId is whitespace or null!");
			this.accessId = accessId;
			return this;
		}

		public Builder setAccessKey(String accessKey) {
			checkArgument(StringUtils.isNotBlank(accessKey), "accessKey is whitespace or null!");
			this.accessKey = accessKey;
			return this;
		}

		public Builder setEndpoint(String endpoint) {
			checkArgument(StringUtils.isNotBlank(endpoint), "endpoint is whitespace or null!");
			this.endpoint = endpoint;
			return this;
		}

		public Builder setProject(String project) {
			checkArgument(StringUtils.isNotBlank(project), "project is whitespace or null! ");
			this.project = project;
			return this;
		}

		public Builder setTable(String table) {
			checkArgument(StringUtils.isNotBlank(table), "table is whitespace or null! ");
			this.table = table;
			return this;
		}

		public Builder setPartitions(String[] partitions) {
			this.partitions = partitions;
			return this;
		}

		public ODPSTableSource build() {
			ODPSConf odpsConf = new ODPSConf(accessId, accessKey, endpoint, project);
			return new ODPSTableSource(odpsConf, table, partitions);
		}
	}
}
