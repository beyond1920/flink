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

import com.alibaba.blink.odps.schema.ODPSColumn;
import com.alibaba.blink.odps.schema.ODPSTableSchema;
import com.alibaba.blink.odps.split.ODPSInputSplit;
import com.alibaba.blink.odps.split.ODPSPartitionSegmentDownloadDesc;
import com.aliyun.odps.Column;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.TunnelRecordReader;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import org.apache.commons.io.IOUtils;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * A reader to read the content of an odps inputSplit
 */
@Internal
public class ODPSInputSplitReader implements Closeable {

	private final static Logger LOGGER = LoggerFactory.getLogger(ODPSInputSplitReader.class);

	private final ODPSPartitionSegmentDownloadDesc[] partSegments;

	private final List<Column> odpsColumns;

	private final TableTunnel tableTunnel;

	private final String project;

	private final String table;

	private TunnelRecordReader recordReader;

	private int currentPartSegmentIdx;

	private ODPSPartitionSegmentDownloadDesc currentPartitionDownloadDesc;

	private PartitionSpec currentPartitionSpec;

	private boolean reachEnd;

	private int position;

	private final int MAX_RETRY_NUMBER = 3;

	/**
	 * create a special reader instance to read the content of an odps inputSplit
	 *
	 * @param split
	 * @param selectedColumns
	 * @param tableTunnel
	 * @param project
	 * @param table
	 */
	public ODPSInputSplitReader(
			ODPSInputSplit split, String[] selectedColumns,
			final ODPSTableSchema tableSchema, TableTunnel tableTunnel, String project,
			final String table)
	{
		this.partSegments = split.getPartitions();
		// filter partition column
		this.odpsColumns = FluentIterable.of(selectedColumns)
				.filter(new Predicate<String>() {
					@Override
					public boolean apply(@Nullable String columnName) {
						return !tableSchema.isPartitionColumn(columnName);
					}
				}).transform(new Function<String, Column>() {
					@Override
					public Column apply(String columnName) {
						ODPSColumn column = tableSchema.getColumn(columnName);
						return new Column(column.getName(), column.getType());
					}
				}).toList();
		this.tableTunnel = tableTunnel;
		this.project = project;
		this.table = table;
		reachEnd = false;
		currentPartSegmentIdx = -1;
		position = 0;
		openNextPartSegment();
	}

	/**
	 * whether has next element of current reader
	 *
	 * @return true if has next element, else return false
	 */
	public boolean hasNext() {
		return !reachEnd;
	}

	/**
	 * return next element
	 *
	 * @return a tuple contain two elements, first is record value of normal columns, second is
	 * partition spec which holds value of partition columns
	 * @throws IOException
	 */
	public Tuple2<Record, PartitionSpec> next() throws IOException {
		Record record = null;
		try {
			record = recordReader.read();
			++position;
		} catch (IOException ioe) {
			LOGGER.warn("Occur Odps record reader issues, reconnect", ioe);
			// reconnect from position
			recordReader = createRecordReader();
			return next();
		}

		if (record != null) {
			return Tuple2.of(record, currentPartitionSpec);
		} else {
			IOUtils.closeQuietly(recordReader);
			if (openNextPartSegment()) {
				return next();
			} else {
				// no partition segment left to read
				return null;
			}
		}
	}

	@Override
	public void close() {
		if (recordReader != null) {
			IOUtils.closeQuietly(recordReader);
			recordReader = null;
		}
	}

	private TunnelRecordReader createRecordReader() {
		int retries = 0;
		while (true) {
			if (retries++ >= MAX_RETRY_NUMBER) {
				LOGGER.warn("reach max retries limit to open record reader");
				throw new RuntimeException(
						"fail to open odps record reader of table for " + MAX_RETRY_NUMBER + "times"
				);
			}

			try {
				TableTunnel.DownloadSession downloadSession = null;
				if (currentPartitionSpec == null) {
					downloadSession = tableTunnel.getDownloadSession(
							project,
							table,
							currentPartitionDownloadDesc.getDownloadSessionID());
				} else {
					downloadSession = tableTunnel.getDownloadSession(
							project,
							table,
							currentPartitionSpec,
							currentPartitionDownloadDesc.getDownloadSessionID());
				}
				return downloadSession.openRecordReader(
						currentPartitionDownloadDesc.getStart() + position,
						currentPartitionDownloadDesc.getCount() - position,
						true,
						odpsColumns);
			} catch (TunnelException | IOException e) {
				if (currentPartitionSpec == null) {
					LOGGER.warn("fail to open record reader of odps table [{}]", table);
				} else {
					LOGGER.warn(
							"fail to open record reader of odps table [{}] on partition [{}]",
							table,
							currentPartitionSpec.toString());
				}

				try {
					Thread.sleep(1000 * retries);
				} catch (InterruptedException ie) {
					LOGGER.warn("fail to open record reader of table", ie);
					throw new RuntimeException("fail to open record reader of table", ie);
				}
			}
		}
	}

	/**
	 * try to read next partition segment
	 *
	 * @return true if open next partition segment, false if there is no next partition segment
	 */
	private boolean openNextPartSegment() {
		if (currentPartSegmentIdx == partSegments.length - 1) {
			reachEnd = true;
			return false;
		} else {
			currentPartitionDownloadDesc = partSegments[++currentPartSegmentIdx];
			currentPartitionSpec = currentPartitionDownloadDesc.getPartitionSpec();
			// skip empty segment
			if (currentPartitionDownloadDesc.getCount() != 0) {
				position = 0;
				recordReader = createRecordReader();
				return true;
			} else {
				return openNextPartSegment();
			}
		}
	}

}

