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

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Table;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.plan.stats.ColumnStats;
import org.apache.flink.table.plan.stats.TablePartitionStats;
import org.apache.flink.table.plan.stats.TableStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class is responsible for providing statistics for Odps table and Odps partition.
 * Note: Only support rowCount currently.
 */
public class OdpsStatsProvider {

	private static final Logger LOGGER = LoggerFactory.getLogger(OdpsStatsProvider.class);
	private final Odps odps;
	private final TableTunnel tunnel;
	private static final int MAX_RETRY_TIMES = 5;

	public OdpsStatsProvider(Odps odps) {
		this.odps = odps;
		this.tunnel = new TableTunnel(odps);
	}

	/**
	 * request statistics of Odps table
	 * Note:
	 * a RuntimeException will happen if catch OdpsException or TunnelException
	 *
	 * @param project project name
	 * @param table   table name
	 * @return table statistics.
	 */
	public TableStats requestStatsOfNonPartitionTable(String project, String table) {
		checkArgument(
				StringUtils.isNotBlank(project),
				"input project cannot be whitespace or null!");
		checkArgument(
				StringUtils.isNotBlank(table),
				"input table cannot be whitespace or null!");
		Table odpsTable = odps.tables().get(project, table);
		checkArgument(!odpsTable.isVirtualView(), "View is not supported yet!");
		TableTunnel.DownloadSession downloadSession =
				retryableCreateDownLoadSession(project, table, null);
		long rowCount = downloadSession.getRecordCount();
		TableStats stats = new TableStats(rowCount, new HashMap<String, ColumnStats>());
		return stats;
	}

	/**
	 * request statistics of Odps table partition
	 * Note:
	 * an IllegalArgumentException will happen if the table is not a partitioned table or partition
	 * specification is invalid.
	 * a RuntimeException will happen if catch OdpsException or TunnelException
	 *
	 * @param project       project name
	 * @param table         table name
	 * @param partitionSpec partition specification
	 * @return partition statistics.
	 */
	public TablePartitionStats requestStatsOfTablePartition(
			String project,
			String table,
			PartitionSpec partitionSpec)
	{
		checkArgument(
				StringUtils.isNotBlank(project),
				"input project cannot be whitespace or null!");
		checkArgument(
				StringUtils.isNotBlank(table),
				"input table cannot be whitespace or null!");
		checkNotNull(partitionSpec, "input partition specification cannot be null!");
		Table odpsTable = odps.tables().get(project, table);
		try {
			checkArgument(odpsTable.isPartitioned(), "The table is not a partitioned table!");
		} catch (OdpsException e) {
			logAndPropagateException(e, "Fail to get odps table %s.%s", project, table);
		}
		try {
			checkArgument(odpsTable.hasPartition(partitionSpec),
					"The specified partitionspec is not valid!");
		} catch (OdpsException e) {
			logAndPropagateException(e,
					"Fail to get partition %s of odps table %s.%s",
					partitionSpec.toString(), project, table);
		}
		TableTunnel.DownloadSession downloadSession =
				retryableCreateDownLoadSession(project, table, partitionSpec);
		long rowCount = downloadSession.getRecordCount();
		TablePartitionStats stats = new TablePartitionStats(
				rowCount, new HashMap<String, ColumnStats>());
		return stats;
	}

	private TableTunnel.DownloadSession retryableCreateDownLoadSession(
			String project,
			String table,
			PartitionSpec partitionSpec)
	{
		int tryTimes = 0;
		while (true) {
			try {
				TableTunnel.DownloadSession downloadSession = null;
				if (partitionSpec == null) {
					downloadSession = tunnel.createDownloadSession(
							project,
							table);
				} else {
					downloadSession = tunnel.createDownloadSession(
							project,
							table,
							partitionSpec);
				}
				return downloadSession;
			} catch (TunnelException e) {
				String downloadObjectStr = null;
				if (partitionSpec == null) {
					downloadObjectStr = String.format("odps table [%s].[%s]!", project, table);
				} else {
					downloadObjectStr = String.format("partition [%s] of odps table [%s].[%s]!",
							partitionSpec.toString(),
							project,
							table);
				}
				if (tryTimes++ >= MAX_RETRY_TIMES) {
					logAndPropagateException(e,
							"Give up to create download session of %s after try %d times",
							downloadObjectStr,
							MAX_RETRY_TIMES);
				} else {
					LOGGER.warn("Retry to create download session of {} after try {} times",
							downloadObjectStr,
							tryTimes, e);
					try {
						TimeUnit.SECONDS.sleep(tryTimes);
					} catch (InterruptedException ie) {
						logAndPropagateException(ie,
								"Stop to create download session of %s because of interruption",
								downloadObjectStr);
					}
				}
			}
		}
	}

	private void logAndPropagateException(Throwable t, String format, Object... arguments) {
		String warnMsg = String.format(format, arguments);
		LOGGER.warn(warnMsg, t);
		throw new RuntimeException(warnMsg, t);
	}
}
