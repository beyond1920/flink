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

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A utility to divide total counts into numSplits parts as equal as possible.
 */
public class ElementSplitUtil {

	/**
	 * sum the input counts, and divide total count into numSplits parts as equal as possible
	 *
	 * @param elementCounts input counts
	 * @param numSplits     specify how many parts to divide into
	 * @return two dimension array which length is numSplits, and every element in these array
	 * contain segments
	 */
	public static ElementSegment[][] doSplit(long[] elementCounts, int numSplits) {
		checkArgument(elementCounts != null && allPositive(elementCounts),
				"element counts is null or contain illegal element! ");
		checkArgument(numSplits > 0, "num splits is less than 1! ");
		int numElements = elementCounts.length;
		if(numElements == 0) {
			return new ElementSegment[numSplits][0];
		}
		long totalCount = 0;
		// generate <element absolute start position , element absolute end position> arrays
		long[][] elementBoundaries = new long[numElements][2];
		for (int idx = 0; idx < numElements; idx++) {
			long[] elementBoundary = new long[2];
			elementBoundary[0] = totalCount;
			totalCount += elementCounts[idx];
			elementBoundary[1] = totalCount - 1;
			elementBoundaries[idx] = elementBoundary;
		}
		long averageSplitCount = totalCount / numSplits;
		ElementSegment[][] splitResult = new ElementSegment[numSplits][];
		for (int splitIdx = 0; splitIdx < numSplits; splitIdx++) {
			long splitStartAbsolutePos = splitIdx * averageSplitCount;
			long splitCount = splitIdx < numSplits - 1 ? averageSplitCount :
					averageSplitCount + totalCount % numSplits;
			long splitEndAbsolutePos = splitStartAbsolutePos + splitCount - 1;

			int startElementId = floorElementLocation(elementBoundaries, splitStartAbsolutePos);
			int endElementId = floorElementLocation(elementBoundaries, splitEndAbsolutePos);
			int elementNums = endElementId + 1 - startElementId;
			ElementSegment[] elementsInOneSplit = new ElementSegment[elementNums];
			for (int partId = startElementId; partId <= endElementId; partId++) {
				long startAbsolutePosition = splitStartAbsolutePos < elementBoundaries[partId][0] ?
						elementBoundaries[partId][0] : splitStartAbsolutePos;
				long endAbsolutePosition = splitEndAbsolutePos < elementBoundaries[partId][1] ?
						splitEndAbsolutePos : elementBoundaries[partId][1];
				long startRelativePosition = startAbsolutePosition - elementBoundaries[partId][0];
				long partCount = endAbsolutePosition + 1 - startAbsolutePosition;
				elementsInOneSplit[partId - startElementId] =
						new ElementSegment(partId, startRelativePosition, partCount);
			}
			splitResult[splitIdx] = elementsInOneSplit;
		}
		return splitResult;
	}

	private static boolean allPositive(long[] elementCounts) {
		for (long count : elementCounts) {
			if (count < 0) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Gets the position is corresponding to the specified value; if no such value exists, returns
	 * the position for the greatest value less than the specified value
	 *
	 * @param elements
	 * @param toSearch
	 * @return
	 */
	private static int floorElementLocation(long[][] elements, long toSearch) {
		int len = elements.length;
		int left = 0;
		int right = len - 1;
		int mid;
		while (left <= right) {
			mid = (left + right) >>> 1;
			if (elements[mid][0] < toSearch) {
				left = left + 1;
			} else if (elements[mid][0] == toSearch) {
				return mid;
			} else {
				right = right - 1;
			}
		}
		return right;
	}

	private ElementSplitUtil() {

	}

	public static class ElementSegment {
		private final int elementId;
		private final long start;
		private final long count;

		public ElementSegment(int elementId, long start, long count) {
			this.elementId = elementId;
			this.start = start;
			this.count = count;
		}

		public int getElementId() {
			return elementId;
		}

		public long getStart() {
			return start;
		}

		public long getCount() {
			return count;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			ElementSegment that = (ElementSegment) o;

			if (elementId != that.elementId) {
				return false;
			}
			if (start != that.start) {
				return false;
			}
			return count == that.count;

		}

		@Override
		public int hashCode() {
			int result = elementId;
			result = 31 * result + (int) (start ^ (start >>> 32));
			result = 31 * result + (int) (count ^ (count >>> 32));
			return result;
		}

		@Override
		public String toString() {
			return "ElementSegment{" +
					"elementId=" + elementId +
					", start=" + start +
					", count=" + count +
					'}';
		}
	}
}
