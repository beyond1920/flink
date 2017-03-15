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

package com.alibaba.blink.odps.util;

import org.junit.Assert;
import org.junit.Test;

public class ElementSplitUtilTest {

	@Test
	public void doSplit() throws Exception {

		ElementSplitUtil.ElementSegment[][] actualSegments =
				ElementSplitUtil.doSplit(new long[]{1L, 1L, 1L}, 4);

		ElementSplitUtil.ElementSegment[][] expectSegments =
				new ElementSplitUtil.ElementSegment[4][];
		expectSegments[0] = new ElementSplitUtil.ElementSegment[0];
		expectSegments[1] = new ElementSplitUtil.ElementSegment[0];
		expectSegments[2] = new ElementSplitUtil.ElementSegment[0];
		expectSegments[3] = new ElementSplitUtil.ElementSegment[] {
				new ElementSplitUtil.ElementSegment(0, 0L, 1L),
				new ElementSplitUtil.ElementSegment(1, 0L, 1L),
				new ElementSplitUtil.ElementSegment(2, 0L, 1L)
		};

		Assert.assertArrayEquals(expectSegments, actualSegments);

		long[] inputCounts1 = new long[]{2L, 1L, 4L};

		ElementSplitUtil.ElementSegment[][] actualSegments1 =
				ElementSplitUtil.doSplit(inputCounts1, 1);

		ElementSplitUtil.ElementSegment[][] expectSegments1 =
				new ElementSplitUtil.ElementSegment[1][];
		expectSegments1[0] = new ElementSplitUtil.ElementSegment[] {
				new ElementSplitUtil.ElementSegment(0, 0L, 2L),
				new ElementSplitUtil.ElementSegment(1, 0L, 1L),
				new ElementSplitUtil.ElementSegment(2, 0L, 4L)
		};

		Assert.assertArrayEquals(expectSegments1, actualSegments1);

		long[] inputCounts = new long[]{2L, 1L, 4L};

		ElementSplitUtil.ElementSegment[][] actualSegments2 =
				ElementSplitUtil.doSplit(inputCounts, 2);


		ElementSplitUtil.ElementSegment[][] expectSegments2 =
				new ElementSplitUtil.ElementSegment[2][];
		expectSegments2[0] = new ElementSplitUtil.ElementSegment[] {
				new ElementSplitUtil.ElementSegment(0, 0L, 2L),
				new ElementSplitUtil.ElementSegment(1, 0L, 1L),
		};
		expectSegments2[1] = new ElementSplitUtil.ElementSegment[] {
				new ElementSplitUtil.ElementSegment(2, 0L, 4L)
		};

		Assert.assertArrayEquals(expectSegments2, actualSegments2);

		ElementSplitUtil.ElementSegment[][] actualSegments3 =
				ElementSplitUtil.doSplit(inputCounts, 3);

		ElementSplitUtil.ElementSegment[][] expectSegments3 =
				new ElementSplitUtil.ElementSegment[3][];
		expectSegments3[0] = new ElementSplitUtil.ElementSegment[] {
				new ElementSplitUtil.ElementSegment(0, 0L, 2L)
		};
		expectSegments3[1] = new ElementSplitUtil.ElementSegment[] {
				new ElementSplitUtil.ElementSegment(1, 0L, 1L),
				new ElementSplitUtil.ElementSegment(2, 0L, 1L),
		};
		expectSegments3[2] = new ElementSplitUtil.ElementSegment[] {
				new ElementSplitUtil.ElementSegment(2, 1L, 3L)
		};
		Assert.assertArrayEquals(expectSegments3, actualSegments3);
	}
}
