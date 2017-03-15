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

package com.alibaba.blink.odps.stat;

import org.apache.flink.api.common.io.statistics.BaseStatistics;

/**
 * OPDS Statistics
 */
public class ODPSStatistics implements BaseStatistics {
	/** the total size of the odps input */
	private final long totalInputSize;

	/** the number of records in the odps input */
	private final long numberOfRecords;

	/** last modify time of the odps input */
	private final long dataLastModTime;

	public ODPSStatistics(long totalInputSize, long numberOfRecords, long dataLastModTime) {
		this.numberOfRecords = numberOfRecords;
		this.totalInputSize = totalInputSize;
		this.dataLastModTime = dataLastModTime;
	}

	@Override
	public long getTotalInputSize() {
		return totalInputSize;
	}

	@Override
	public long getNumberOfRecords() {
		return numberOfRecords;
	}

	@Override
	public float getAverageRecordWidth() {
		return totalInputSize / numberOfRecords;
	}

	public long getDataLastModTime() {
		return dataLastModTime;
	}
}
