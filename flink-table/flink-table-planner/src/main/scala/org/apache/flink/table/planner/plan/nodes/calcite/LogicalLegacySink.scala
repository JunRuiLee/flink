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
package org.apache.flink.table.planner.plan.nodes.calcite

import org.apache.flink.table.catalog.CatalogTable
import org.apache.flink.table.legacy.sinks.TableSink

import org.apache.calcite.plan.{Convention, RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.hint.RelHint

import java.util

import scala.collection.JavaConversions._

/**
 * Sub-class of [[LegacySink]] that is a relational expression which writes out data of input node
 * into a [[TableSink]]. This class corresponds to Calcite logical rel.
 */
final class LogicalLegacySink(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    hints: util.List[RelHint],
    sink: TableSink[_],
    sinkName: String,
    val catalogTable: CatalogTable,
    val staticPartitions: Map[String, String])
  extends LegacySink(cluster, traitSet, input, hints, sink, sinkName) {

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new LogicalLegacySink(
      cluster,
      traitSet,
      inputs.head,
      hints,
      sink,
      sinkName,
      catalogTable,
      staticPartitions)
  }

}

object LogicalLegacySink {

  def create(
      input: RelNode,
      hints: util.List[RelHint],
      sink: TableSink[_],
      sinkName: String,
      catalogTable: CatalogTable,
      staticPartitions: Map[String, String]): LogicalLegacySink = {
    val traits = input.getCluster.traitSetOf(Convention.NONE)
    new LogicalLegacySink(
      input.getCluster,
      traits,
      input,
      hints,
      sink,
      sinkName,
      catalogTable,
      staticPartitions)
  }

  def create(
      input: RelNode,
      sink: TableSink[_],
      sinkName: String,
      catalogTable: CatalogTable = null,
      staticPartitions: Map[String, String] = Map()): LogicalLegacySink = {
    create(input, util.Collections.emptyList(), sink, sinkName, catalogTable, staticPartitions)
  }
}
