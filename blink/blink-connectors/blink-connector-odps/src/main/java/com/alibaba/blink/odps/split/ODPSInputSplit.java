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

import org.apache.flink.core.io.InputSplit;

import java.util.Arrays;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * input doSplit of odps table
 */
public class ODPSInputSplit implements InputSplit {

	private static final long serialVersionUID = -4003215075338027544L;

	private final int splitId;
	private ODPSPartitionSegmentDownloadDesc[] partitions;

	public ODPSInputSplit(int splitId, ODPSPartitionSegmentDownloadDesc... partitions) {
		checkArgument(splitId >= 0, "splitId is less than zero!");
		this.splitId = splitId;
		this.partitions = checkNotNull(partitions, "partitions is null!");
	}

	@Override
	public int getSplitNumber() {
		return splitId;
	}


	public ODPSPartitionSegmentDownloadDesc[] getPartitions() {
		return partitions;
	}

	@Override
	public String toString() {
		return "ODPSInputSplit{" +
				"splitId=" + splitId +
				", partitions=" + Arrays.toString(partitions) +
				'}';
	}
}
