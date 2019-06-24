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

package org.apache.flink.table.api.batch.table.stringexpr

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.util.{HierarchyTableFunction, PojoTableFunc, TableFunc1, TableFunc2, TableTestBase}

import org.junit.Test

class CorrelateStringExpressionTest extends TableTestBase {

  private val util = batchTestUtil()
  private val tab = util.addTableSource[(Int, Long, String)]("Table1", 'a, 'b, 'c)
  private val func1 = new TableFunc1
  util.tableEnv.registerFunction("func1", func1)
  private val func2 = new TableFunc2
  util.tableEnv.registerFunction("func2", func2)

  @Test
  def testCorrelateJoins1(): Unit = {
    // test cross join
    val scalaTable = tab.joinLateral(func1('c) as 's).select('c, 's)
    val javaTable = tab.joinLateral("func1(c).as(s)").select("c, s")
    verifyTableEquals(scalaTable, javaTable)
    util.verifyPlan(scalaTable)
  }

  @Test
  def testCorrelateJoins2(): Unit = {
    // test left outer join
    val scalaTable = tab.leftOuterJoinLateral(func1('c) as 's).select('c, 's)
    val javaTable = tab.leftOuterJoinLateral("as(func1(c), s)").select("c, s")
    verifyTableEquals(scalaTable, javaTable)
    util.verifyPlan(scalaTable)
  }

  @Test
  def testCorrelateJoins3(): Unit = {
    // test overloading
    val scalaTable = tab.joinLateral(func1('c, "$") as 's).select('c, 's)
    val javaTable = tab.joinLateral("func1(c, '$') as (s)").select("c, s")
    verifyTableEquals(scalaTable, javaTable)
    util.verifyPlan(scalaTable)
  }

  @Test
  def testCorrelateJoins4(): Unit = {
    // test custom result type
    val scalaTable = tab.joinLateral(func2('c) as('name, 'len)).select('c, 'name, 'len)
    val javaTable = tab.joinLateral(
      "func2(c).as(name, len)").select("c, name, len")
    verifyTableEquals(scalaTable, javaTable)
    util.verifyPlan(scalaTable)
  }

  @Test
  def testCorrelateJoins5(): Unit = {
    // test hierarchy generic type
    val hierarchy = new HierarchyTableFunction
    util.tableEnv.registerFunction("hierarchy", hierarchy)
    val scalaTable = tab.joinLateral(
      hierarchy('c) as('name, 'adult, 'len)).select('c, 'name, 'len, 'adult)
    val javaTable = tab.joinLateral("AS(hierarchy(c), name, adult, len)")
      .select("c, name, len, adult")
    verifyTableEquals(scalaTable, javaTable)
    util.verifyPlan(scalaTable)
  }

  @Test
  def testCorrelateJoins6(): Unit = {
    // test pojo type
    val pojo = new PojoTableFunc
    util.tableEnv.registerFunction("pojo", pojo)
    val scalaTable = tab.joinLateral(pojo('c)).select('c, 'name, 'age)
    val javaTable = tab.joinLateral("pojo(c)").select("c, name, age")
    util.verifyPlan(scalaTable)
  }

  @Test
  def testCorrelateJoins7(): Unit = {
    // test with filter
    val scalaTable = tab.joinLateral(
      func2('c) as('name, 'len)).select('c, 'name, 'len).filter('len > 2)
    val javaTable = tab.joinLateral("func2(c) as (name, len)")
      .select("c, name, len").filter("len > 2")
    verifyTableEquals(scalaTable, javaTable)
    util.verifyPlan(scalaTable)
  }

  @Test
  def testCorrelateJoins8(): Unit = {
    // test with scalar function
    val scalaTable = tab.joinLateral(func1('c.substring(2)) as 's).select('a, 'c, 's)
    val javaTable = tab.joinLateral(
      "func1(substring(c, 2)) as (s)").select("a, c, s")
    verifyTableEquals(scalaTable, javaTable)
    util.verifyPlan(scalaTable)
  }
}
