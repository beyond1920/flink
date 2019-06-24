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

package org.apache.flink.table.api.stream.table

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{Session, Slide, Tumble}
import org.apache.flink.table.util.{EmptyTableAggFunc, TableTestBase}

import org.junit.{Ignore, Test}

@Ignore
class GroupWindowTableAggregateTest extends TableTestBase {

  val util = streamTestUtil()
  val table = util.addDataStream[(Long, Int, Long, Long)]("T1", 'a, 'b, 'c, 'rowtime, 'proctime)
  val emptyFunc = new EmptyTableAggFunc

  @Test
  def testMultiWindow(): Unit = {
    val windowedTable = table
      .window(Tumble over 50.milli on 'e as 'w1)
      .groupBy('w1, 'c)
      .flatAggregate(emptyFunc('a, 'b))
      .select('w1.proctime as 'proctime, 'c, 'f0, 'f1 + 1 as 'f1)
      .window(Slide over 20.milli every 10.milli on 'proctime as 'w2)
      .groupBy('w2)
      .flatAggregate(emptyFunc('f0))
      .select('w2.start, 'f1)
    util.verifyPlan(windowedTable)
  }

  @Test
  def testProcessingTimeTumblingGroupWindowOverTime(): Unit = {

    val windowedTable = table
      .window(Tumble over 50.milli on 'e as 'w1)
      .groupBy('w1, 'b % 5 as 'bb)
      .flatAggregate(emptyFunc('a, 'b) as ('x, 'y))
      .select('w1.proctime as 'proctime, 'bb, 'x + 1, 'y)
    util.verifyPlan(windowedTable)
  }

  @Test
  def testProcessingTimeTumblingGroupWindowOverCount(): Unit = {
    val windowedTable = table
      .window(Tumble over 2.rows on 'e as 'w)
      .groupBy('w, 'c)
      .flatAggregate(emptyFunc('a, 'b))
      .select('c, 'f0, 'f1)
    util.verifyPlan(windowedTable)
  }

  @Test
  def testEventTimeTumblingGroupWindowOverTime(): Unit = {
    val windowedTable = table
      .window(Tumble over 5.milli on 'd as 'w)
      .groupBy('w, 'c)
      .flatAggregate(emptyFunc('a, 'b))
      .select('c, 'f0, 'f1)
    util.verifyPlan(windowedTable)
  }

  @Test
  def testProcessingTimeSlidingGroupWindowOverTime(): Unit = {
    val windowedTable = table
      .window(Slide over 50.milli every 50.milli on 'e as 'w)
      .groupBy('w, 'c)
      .flatAggregate(emptyFunc('a, 'b))
      .select('w.proctime as 'proctime, 'c, 'f0, 'f1 + 1)
    util.verifyPlan(windowedTable)
  }

  @Test
  def testProcessingTimeSlidingGroupWindowOverCount(): Unit = {
    val windowedTable = table
      .window(Slide over 2.rows every 1.rows on 'e as 'w)
      .groupBy('w, 'c)
      .flatAggregate(emptyFunc('a, 'b))
      .select('c, 'f0, 'f1)

    util.verifyPlan(windowedTable)
  }

  @Test
  def testEventTimeSlidingGroupWindowOverTime(): Unit = {
    val windowedTable = table
      .window(Slide over 8.milli every 10.milli on 'd as 'w)
      .groupBy('w, 'c)
      .flatAggregate(emptyFunc('a, 'b))
      .select('c, 'f0, 'f1)
    util.verifyPlan(windowedTable)
  }

  @Test
  def testEventTimeSessionGroupWindowOverTime(): Unit = {
    val windowedTable = table
      .window(Session withGap 7.milli on 'd as 'w)
      .groupBy('w, 'c)
      .flatAggregate(emptyFunc('a, 'b))
      .select('c, 'f0, 'f1)
    util.verifyPlan(windowedTable)
  }

  @Test
  def testAllProcessingTimeTumblingGroupWindowOverTime(): Unit = {
    val windowedTable = table
      .window(Tumble over 50.milli on 'e as 'w)
      .groupBy('w)
      .flatAggregate(emptyFunc('a, 'b))
      .select('f0, 'f1)
    util.verifyPlan(windowedTable)
  }

  @Test
  def testAllProcessingTimeTumblingGroupWindowOverCount(): Unit = {
    val windowedTable = table
      .window(Tumble over 2.rows on 'e as 'w)
      .groupBy('w)
      .flatAggregate(emptyFunc('a, 'b))
      .select('f0, 'f1)
    util.verifyPlan(windowedTable)
  }

  @Test
  def testAllEventTimeTumblingGroupWindowOverTime(): Unit = {
    val windowedTable = table
      .window(Tumble over 5.milli on 'd as 'w)
      .groupBy('w)
      .flatAggregate(emptyFunc('a, 'b))
      .select('f0, 'f1)
    util.verifyPlan(windowedTable)
  }

  @Test
  def testAllProcessingTimeSlidingGroupWindowOverTime(): Unit = {
    val windowedTable = table
      .window(Slide over 50.milli every 50.milli on 'e as 'w)
      .groupBy('w)
      .flatAggregate(emptyFunc('a, 'b))
      .select('f0, 'f1)
    util.verifyPlan(windowedTable)
  }

  @Test
  def testAllProcessingTimeSlidingGroupWindowOverCount(): Unit = {
    val windowedTable = table
      .window(Slide over 2.rows every 1.rows on 'e as 'w)
      .groupBy('w)
      .flatAggregate(emptyFunc('a, 'b))
      .select('f0, 'f1)
    util.verifyPlan(windowedTable)
  }

  @Test
  def testAllEventTimeSlidingGroupWindowOverTime(): Unit = {
    val windowedTable = table
      .window(Slide over 8.milli every 10.milli on 'd as 'w)
      .groupBy('w)
      .flatAggregate(emptyFunc('a, 'b))
      .select('f0, 'f1)
    util.verifyPlan(windowedTable)
  }

  @Test
  def testAllEventTimeSlidingGroupWindowOverCount(): Unit = {
    val windowedTable = table
      .window(Slide over 8.milli every 10.milli on 'd as 'w)
      .groupBy('w)
      .flatAggregate(emptyFunc('a, 'b))
      .select('f0, 'f1)
    util.verifyPlan(windowedTable)
  }

  @Test
  def testAllEventTimeSessionGroupWindowOverTime(): Unit = {
    val windowedTable = table
      .window(Session withGap 7.milli on 'd as 'w)
      .groupBy('w)
      .flatAggregate(emptyFunc('a, 'b))
      .select('f0, 'f1)
    util.verifyPlan(windowedTable)
  }

  @Test
  def testTumbleWindowStartEnd(): Unit = {
    val windowedTable = table
      .window(Tumble over 5.milli on 'd as 'w)
      .groupBy('w, 'c)
      .flatAggregate(emptyFunc('a, 'b))
      .select('f0, 'f1 + 1, 'w.start, 'w.end)
    util.verifyPlan(windowedTable)
  }

  @Test
  def testSlideWindowStartEnd(): Unit = {
    val windowedTable = table
      .window(Slide over 10.milli every 5.milli on 'd as 'w)
      .groupBy('w, 'c)
      .flatAggregate(emptyFunc('a, 'b))
      .select('f0, 'f1 + 1, 'w.start, 'w.end)

    util.verifyPlan(windowedTable)
  }

  @Test
  def testSessionWindowStartWithTwoEnd(): Unit = {
    val windowedTable = table
      .window(Session withGap 3.milli on 'd as 'w)
      .groupBy('w, 'c)
      .flatAggregate(emptyFunc('a, 'b))
      .select('w.end as 'we1, 'f0, 'f1 + 1, 'w.start, 'w.end)
    util.verifyPlan(windowedTable)
  }
}
