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
import com.alibaba.blink.odps.schema.ODPSTableSchema;
import com.alibaba.blink.odps.split.ODPSInputSplit;
import com.alibaba.blink.odps.split.ODPSPartitionSegmentDownloadDesc;
import com.alibaba.blink.odps.stat.ODPSStatistics;
import com.alibaba.blink.odps.util.ElementSplitUtil;
import com.alibaba.blink.odps.util.ODPSUtil;
import com.aliyun.odps.Odps;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.google.common.collect.Iterables;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitAssigner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

@Internal
public abstract class ODPSInputFormatBase<T> extends RichInputFormat<T, ODPSInputSplit> {

	private static final Logger LOGGER = LoggerFactory.getLogger(ODPSInputFormatBase.class);

	private final ODPSConf odpsConf;

	private final String table;

	/** partition to prune */
	private final String[] partitions;

	/** columns to prune */
	protected final String[] selectedColumns;

	/** odps table schema information */
	protected final ODPSTableSchema odpsTableSchema;

	private Tuple2<String[], long[]> downloadSessionsAndCounts;

	private transient ODPSInputSplitReader reader;

	private transient TableTunnel tableTunnel;


	public ODPSInputFormatBase(ODPSConf odpsConf, String table, String[] selectedColumns,
			ODPSTableSchema tableSchema, @Nullable String[] partitions)
	{
		this.odpsConf = checkNotNull(odpsConf);
		checkArgument(ArrayUtils.isNotEmpty(selectedColumns),
				"input selected columns cannot be null or empty! ");
		this.selectedColumns = selectedColumns;
		checkArgument(StringUtils.isNotBlank(table), "table is whitespace or null! ");
		checkArgument(!tableSchema.isPartition() || partitions != null,
				"partitions cannot be null for partition table !");
		this.odpsTableSchema = tableSchema;
		this.table = table;
		this.partitions = partitions;
	}

	@Override
	public void configure(Configuration parameters) {
		// TODO
	}

	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
		final ODPSStatistics cachedStats =
				(cachedStatistics != null && cachedStatistics instanceof ODPSStatistics) ?
						(ODPSStatistics) cachedStatistics : null;
		try {
			long dataLastModTime = 0;
			if (!odpsTableSchema.isPartition()) {
				long tableDataLastModTime = ODPSUtil.getTableLastModifyTime(odpsConf, table);
				dataLastModTime = Math.max(tableDataLastModTime, dataLastModTime);
			} else {
				for (String partition : partitions) {
					long partitionDataLastModTime =
							ODPSUtil.getPartitionLastModifyTime(odpsConf, table, partition);
					dataLastModTime = Math.max(partitionDataLastModTime, dataLastModTime);
				}
			}
			// if data didn't changed since last time, return last statistics
			if (cachedStats != null && dataLastModTime <= cachedStats.getDataLastModTime()) {
				return cachedStats;
			}

			long totalSize = 0;
			if (!odpsTableSchema.isPartition()) {
				totalSize = ODPSUtil.getTableSize(odpsConf, table);
			} else {
				for (String partition : partitions) {
					totalSize += ODPSUtil.getPartitionSize(odpsConf, table, partition);
				}
			}

			setDownloadSessionAndCountIfAbsent();
			long[] numberOfRecords = downloadSessionsAndCounts.f1;
			long totalNumberOfRecords = 0;
			for (long nums : numberOfRecords) {
				totalNumberOfRecords += nums;
			}

			return new ODPSStatistics(totalSize, totalNumberOfRecords, dataLastModTime);
		} catch (Throwable t) {
			LOGGER.error("fail to get statistics of odps table [{}]", table, t);
			// no statistics available
			return null;
		}
	}

	@Override
	public void openInputFormat() {
		// called once per inputFormat (on open) on DataSourceTask
		Odps odps = ODPSUtil.initOdps(odpsConf);
		this.tableTunnel = new TableTunnel(odps);
	}

	@Override
	public void closeInputFormat() {
		tableTunnel = null;
	}

	@Override
	public ODPSInputSplit[] createInputSplits(int minNumSplits) throws IOException {
		checkArgument(minNumSplits >= 1, "splits num cannot be less than 1!");
		int numSplits = minNumSplits;
		// divide total count into numSplits parts
		// maybe one task would process more than one partition segment
		setDownloadSessionAndCountIfAbsent();
		long[] counts = downloadSessionsAndCounts.f1;
		ElementSplitUtil.ElementSegment[][] splitsResult = ElementSplitUtil
				.doSplit(counts, numSplits);
		assert splitsResult.length == numSplits;
		String[] sessionIds = downloadSessionsAndCounts.f0;
		ODPSInputSplit[] splits = new ODPSInputSplit[numSplits];
		for (int splitIdx = 0; splitIdx < numSplits; splitIdx++) {
			ElementSplitUtil.ElementSegment[] segmentsInOneSplit = splitsResult[splitIdx];
			List<ODPSPartitionSegmentDownloadDesc> partitionSegments =
					new ArrayList<>(segmentsInOneSplit.length);
			for (int segmentIdx = 0; segmentIdx < segmentsInOneSplit.length; segmentIdx++) {
				ElementSplitUtil.ElementSegment segment = segmentsInOneSplit[segmentIdx];
				// filter segment which count is less than 0
				if(segment.getCount() <= 0) {
					continue;
				}
				int partitionId = segment.getElementId();
				String partition = odpsTableSchema.isPartition() ? partitions[partitionId] : null;
				String sessionId = sessionIds[partitionId];
				partitionSegments.add(new ODPSPartitionSegmentDownloadDesc(
						partition, segment.getStart(), segment.getCount(), sessionId));
			}
			splits[splitIdx] = new ODPSInputSplit(splitIdx,
					Iterables.toArray(partitionSegments, ODPSPartitionSegmentDownloadDesc.class));
		}
		return splits;
	}

	@Override
	public InputSplitAssigner getInputSplitAssigner(ODPSInputSplit[] inputSplits) {
		return new DefaultInputSplitAssigner(inputSplits);
	}

	@Override
	public void open(ODPSInputSplit split) throws IOException {
		this.reader = new ODPSInputSplitReader(split, this.selectedColumns, odpsTableSchema,
				tableTunnel, this.odpsConf.getProject(), table);
	}

	@Override
	public boolean reachedEnd() throws IOException {
		return !this.reader.hasNext();
	}

	@Override
	public T nextRecord(T reuse) throws IOException {
		Tuple2<Record, PartitionSpec> columnContent = this.reader.next();
		if (columnContent == null) {
			IOUtils.closeQuietly(this.reader);
			return null;
		} else {
			return convertRecord(columnContent.f0, columnContent.f1, reuse);
		}
	}

	@Override
	public void close() throws IOException {
		if (this.reader != null) {
			IOUtils.closeQuietly(this.reader);
		}
	}

	/**
	 * convert ODPS record instance to specify T type
	 *
	 * @param record        record instance, not including partition column value
	 * @param partitionSpec partition spec of this record
	 * @param reuse         reuse data
	 * @return converted data
	 * @throws IOException
	 */
	protected abstract T convertRecord(Record record, PartitionSpec partitionSpec,
			T reuse) throws IOException;

	/**
	 * get record count of each partition or table and save session id
	 *
	 * @return
	 * @throws IOException
	 */
	private void setDownloadSessionAndCountIfAbsent() throws IOException {
		if (this.downloadSessionsAndCounts != null) {
			return;
		}
		Odps odps = ODPSUtil.initOdps(odpsConf);
		TableTunnel tunnel = new TableTunnel(odps);
		String project = odpsConf.getProject();
		if (!odpsTableSchema.isPartition()) {
			try {
				TableTunnel.DownloadSession downloadSession =
						tunnel.createDownloadSession(project, table);
				this.downloadSessionsAndCounts = new Tuple2<>(new String[]{downloadSession.getId()},
						new long[]{downloadSession.getRecordCount()});
			} catch (TunnelException e) {
				LOGGER.warn("fail to create download session to table [{}] in project [{}]",
						project, e);
				throw new IOException("fail to create download session to odps table", e);
			}
		} else {
			int partitionSize = partitions.length;
			String[] sessionIds = new String[partitionSize];
			long[] counts = new long[partitionSize];
			for (int partitionIdx = 0; partitionIdx < partitionSize; partitionIdx++) {
				try {
					PartitionSpec partitionSpec = new PartitionSpec(partitions[partitionIdx]);
					TableTunnel.DownloadSession downloadSession =
							tunnel.createDownloadSession(project, table, partitionSpec);
					sessionIds[partitionIdx] = downloadSession.getId();
					counts[partitionIdx] = downloadSession.getRecordCount();
				} catch (TunnelException e) {
					LOGGER.warn("fail to create download session to partition [{}] on table [{}] " +
							"in project [{}]", partitions[partitionIdx], table, project, e);
					throw new IOException("fail to create download session to odps table", e);
				}
			}
			this.downloadSessionsAndCounts = new Tuple2<>(sessionIds, counts);
		}
	}
}


