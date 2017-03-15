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

package com.alibaba.blink.odps.inputformat;

import com.alibaba.blink.odps.conf.ODPSConf;
import com.alibaba.blink.odps.schema.ODPSColumn;
import com.alibaba.blink.odps.schema.ODPSTableSchema;
import com.alibaba.blink.odps.type.ODPSType;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.data.Record;
import org.apache.flink.annotation.Internal;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;
import java.io.IOException;


@Internal
public class ODPSInputFormat extends ODPSInputFormatBase<Row> {

	public ODPSInputFormat(ODPSConf odpsConf, String table, String[] selectedColumns,
			ODPSTableSchema tableSchema, @Nullable String[] partitions)
	{
		super(odpsConf, table, selectedColumns, tableSchema, partitions);
	}

	@Override
	protected Row convertRecord(Record record, PartitionSpec partitionSpec,
			Row reuse) throws IOException
	{
		for (int idx = 0; idx < selectedColumns.length; idx++) {
			String columnName = selectedColumns[idx];
			ODPSColumn column = odpsTableSchema.getColumn(columnName);
			ODPSType odpsType = ODPSType.valueOf(column.getType().name());
			if (column.isPartition()) {
				odpsType.setRowField(reuse, idx, partitionSpec.get(columnName));
			} else {
				odpsType.setRowField(reuse, idx, record, columnName);
			}
		}
		return reuse;
	}
}
