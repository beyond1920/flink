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

package org.apache.flink.table.plan.nodes.physical.stream

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSink}
import org.apache.flink.streaming.api.transformations.StreamTransformation
import org.apache.flink.table.api.{StreamTableEnvironment, Table, TableException}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.CodeGenUtils
import org.apache.flink.table.codegen.SinkCodeGenerator.extractTableSinkTypeClass
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.nodes.calcite.Sink
import org.apache.flink.table.plan.nodes.exec.{ExecNode, StreamExecNode}
import org.apache.flink.table.plan.`trait`.{AccMode, AccModeTraitDef}
import org.apache.flink.table.plan.util.UpdatingPlanChecker
import org.apache.flink.table.sinks._

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelNode

import java.util

import scala.collection.JavaConversions._

/**
  * Stream physical RelNode to to write data into an external sink defined by a [[TableSink]].
  */
class StreamExecSink[T](
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    sink: TableSink[T],
    sinkName: String)
  extends Sink(cluster, traitSet, inputRel, sink, sinkName)
  with StreamPhysicalRel
  with StreamExecNode[Any] {

  override def producesUpdates: Boolean = false

  override def needsUpdatesAsRetraction(input: RelNode): Boolean =
    sink.isInstanceOf[BaseRetractStreamTableSink[_]]

  override def consumesRetractions: Boolean = false

  override def producesRetractions: Boolean = false

  override def requireWatermark: Boolean = false

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new StreamExecSink(cluster, traitSet, inputs.get(0), sink, sinkName)
  }

  /**
    * Returns an array of this node's inputs. If there are no inputs,
    * returns an empty list, not null.
    *
    * @return Array of this node's inputs
    */
  override def getInputNodes: util.List[ExecNode[StreamTableEnvironment, _]] = {
    List(getInput.asInstanceOf[ExecNode[StreamTableEnvironment, _]])
  }

  /**
    * Internal method, translates this node into a Flink operator.
    *
    * @param tableEnv The [[StreamTransformation]] of the translated Table.
    */
  override protected def translateToPlanInternal(
    tableEnv: StreamTableEnvironment): StreamTransformation[Any] = {
    val convertTransformation = sink match {

      case _: BaseRetractStreamTableSink[T] =>
        // translate the Table into a DataStream and provide the type that the TableSink expects.
        translate(withChangeFlag = true, tableEnv)

      case upsertSink: BaseUpsertStreamTableSink[T] =>
        // check for append only table
        val isAppendOnlyTable = UpdatingPlanChecker.isAppendOnly(this)
        upsertSink.setIsAppendOnly(isAppendOnlyTable)
        // translate the Table into a DataStream and provide the type that the TableSink expects.
        translate(withChangeFlag = true, tableEnv)

      case _: AppendStreamTableSink[T] =>
        // verify table is an insert-only (append-only) table
        if (!UpdatingPlanChecker.isAppendOnly(this)) {
          throw new TableException(
            "AppendStreamTableSink requires that Table has only insert changes.")
        }

        val accMode = this.getTraitSet.getTrait(AccModeTraitDef.INSTANCE).getAccMode
        if (accMode == AccMode.AccRetract) {
          throw new TableException(
            "AppendStreamTableSink can not be used to output retraction messages.")
        }

        // translate the Table into a DataStream and provide the type that the TableSink expects.
        translate(withChangeFlag = false, tableEnv)

      case s: DataStreamTableSink[_] =>
        translate(s.withChangeFlag, tableEnv)
      case _ =>
        throw new TableException("Stream Tables can only be emitted by AppendStreamTableSink, " +
                                   "RetractStreamTableSink, or UpsertStreamTableSink.")
    }
    val resultTransformation = if (sink.isInstanceOf[DataStreamTableSink[T]]) {
      convertTransformation
    } else {
      val stream = new DataStream(tableEnv.execEnv, convertTransformation)
      emitDataStream(tableEnv.getConfig.getConf, stream).getTransformation
    }
    resultTransformation.asInstanceOf[StreamTransformation[Any]]
  }

  /**
    * Translates a logical [[RelNode]] into a [[StreamTransformation]].
    *
    * @param withChangeFlag Set to true to emit records with change flags.
    * @return The [[StreamTransformation]] that corresponds to the translated [[Table]].
    */
  private def translate(
    withChangeFlag: Boolean,
    tableEnv: StreamTableEnvironment): StreamTransformation[T] = {
    val inputNode = getInput
    val resultType = sink.getOutputType
    // if no change flags are requested, verify table is an insert-only (append-only) table.
    if (!withChangeFlag && !UpdatingPlanChecker.isAppendOnly(inputNode)) {
      throw new TableException(
        "Table is not an append-only table. " +
          "Use the toRetractStream() in order to handle add and retract messages.")
    }

    // get BaseRow plan
    val translateStream = inputNode match {
      // Sink's input must be StreamExecNode[BaseRow] now.
      case node: StreamExecNode[BaseRow] =>
        node.translateToPlan(tableEnv)
      case _ =>
        throw new TableException("Cannot generate DataStream due to an invalid logical plan. " +
                                   "This is a bug and should not happen. Please file an issue.")
    }
    val parTransformation = translateStream
    val logicalType = inputNode.getRowType
    val rowtimeFields = logicalType.getFieldList
                        .filter(f => FlinkTypeFactory.isRowtimeIndicatorType(f.getType))

    if (rowtimeFields.size > 1) {
      throw new TableException(
        s"Found more than one rowtime field: [${rowtimeFields.map(_.getName).mkString(", ")}] in " +
          s"the table that should be converted to a DataStream.\n" +
          s"Please select the rowtime field that should be used as event-time timestamp for the " +
          s"DataStream by casting all other fields to TIMESTAMP.")
    }

    // TODO support SinkConversion after FLINK-11974 is done
    val typeClass = extractTableSinkTypeClass(sink)
    if (CodeGenUtils.isInternalClass(typeClass, resultType)) {
      parTransformation.asInstanceOf[StreamTransformation[T]]
    } else {
      throw new TableException(
        s"Not support SinkConvention now."
      )
    }

  }

  /**
    * emit [[DataStream]].
    *
    * @param dataStream The [[DataStream]] to emit.
    */
  private def emitDataStream(
    tableConf: Configuration,
    dataStream: DataStream[T]) : DataStreamSink[_] = {
    sink match {

      case retractSink: BaseRetractStreamTableSink[T] =>
        // Give the DataStream to the TableSink to emit it.
        retractSink.emitDataStream(dataStream)

      case upsertSink: BaseUpsertStreamTableSink[T] =>
        // Give the DataStream to the TableSink to emit it.
        upsertSink.emitDataStream(dataStream)

      case appendSink: AppendStreamTableSink[T] =>
        // Give the DataStream to the TableSink to emit it.
        appendSink.emitDataStream(dataStream)

      case _ =>
        throw new TableException("Stream Tables can only be emitted by AppendStreamTableSink, " +
                                   "RetractStreamTableSink, or UpsertStreamTableSink.")
    }
  }

}
