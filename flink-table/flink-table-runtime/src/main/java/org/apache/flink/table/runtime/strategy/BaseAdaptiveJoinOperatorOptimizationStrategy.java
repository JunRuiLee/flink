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

package org.apache.flink.table.runtime.strategy;

import org.apache.flink.runtime.scheduler.adaptivebatch.OperatorsFinished;
import org.apache.flink.runtime.scheduler.adaptivebatch.StreamGraphOptimizationStrategy;
import org.apache.flink.streaming.api.graph.StreamGraphContext;
import org.apache.flink.streaming.api.graph.util.*;
import org.apache.flink.table.runtime.operators.join.AdaptiveJoin;

import java.util.List;

/**
 * The base stream graph optimization strategy class for adaptive join operator.
 */
public abstract class BaseAdaptiveJoinOperatorOptimizationStrategy
        implements StreamGraphOptimizationStrategy {

    public void visitDownstreamAdaptiveJoinNode(
            OperatorsFinished operatorsFinished, StreamGraphContext context) {
        ImmutableStreamGraph streamGraph = context.getStreamGraph();
        List<Integer> finishedStreamNodeIds = operatorsFinished.getFinishedStreamNodeIds();
        for (Integer finishedStreamNodeId : finishedStreamNodeIds) {
            for (ImmutableStreamEdge streamEdge :
                    streamGraph.getStreamNode(finishedStreamNodeId).getOutEdges()) {
                ImmutableStreamNode downstreamNode =
                        streamGraph.getStreamNode(streamEdge.getTargetId());
                if (downstreamNode.getOperatorFactory() instanceof AdaptiveJoin) {
                    tryOptimizeAdaptiveJoin(
                            operatorsFinished,
                            context,
                            downstreamNode,
                            streamEdge,
                            (AdaptiveJoin) downstreamNode.getOperatorFactory());
                }
            }
        }
    }

    public abstract void tryOptimizeAdaptiveJoin(
            OperatorsFinished operatorsFinished,
            StreamGraphContext context,
            ImmutableStreamNode adaptiveJoinNode,
            ImmutableStreamEdge upstreamStreamEdge,
            AdaptiveJoin adaptiveJoin);
}
