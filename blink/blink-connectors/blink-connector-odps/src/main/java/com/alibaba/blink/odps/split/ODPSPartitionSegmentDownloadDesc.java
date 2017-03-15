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

package com.alibaba.blink.odps.split;


import com.aliyun.odps.PartitionSpec;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * a description of an odps partition download
 */
public class ODPSPartitionSegmentDownloadDesc implements Serializable {
	private static final long serialVersionUID = -3786613257670817735L;
	/** specify a odps partition */
	private final String partition;

	private transient PartitionSpec partitionSpec;

	/** start location to download of a odps partition */
	private final long start;

	/** count to download from a odps partition */
	private final long count;

	/** download session */
	private String downloadSessionID;

	public ODPSPartitionSegmentDownloadDesc(@Nullable String partition, long start, long count,
			String downloadSessionID)
	{
		checkArgument(StringUtils.isNotBlank(downloadSessionID),
				"downloadSessionID is whitespace or null!");
		checkArgument(start >= 0, "start is less than zero!");
		checkArgument(count > 0, "count is not bigger than zero!");
		this.partition = partition;
		buildPartitionSpec();
		this.start = start;
		this.count = count;
		this.downloadSessionID = downloadSessionID;
	}

	public String getPartition() {
		return partition;
	}

	public long getStart() {
		return start;
	}

	public long getCount() {
		return count;
	}

	public String getDownloadSessionID() {
		return downloadSessionID;
	}

	public void setDownloadSessionID(String downloadSessionID) {
		this.downloadSessionID = downloadSessionID;
	}

	public PartitionSpec getPartitionSpec() {
		return partitionSpec;
	}

	private void readObject(ObjectInputStream inputStream) throws IOException,
			ClassNotFoundException
	{
		inputStream.defaultReadObject();
		buildPartitionSpec();
	}

	private void buildPartitionSpec() {
		this.partitionSpec = partition == null ? null : new PartitionSpec(partition);
	}

	@Override
	public String toString() {
		return "ODPSPartitionSegmentDownloadDesc{" +
				"partition='" + partition + '\'' +
				", partitionSpec=" + partitionSpec +
				", start=" + start +
				", count=" + count +
				", downloadSessionID='" + downloadSessionID + '\'' +
				'}';
	}
}
