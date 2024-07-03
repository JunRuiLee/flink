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

package org.apache.flink.streaming.util;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.StreamOperator;

/** Utilities for creating {@link StreamGraph streamGraphs} for testing purposes. */
public class StreamGraphTestUtils {

    public static StreamGraph emptyStreamGraph() {
        return new StreamGraph(
                new Configuration(),
                new ExecutionConfig(),
                new CheckpointConfig(),
                SavepointRestoreSettings.none());
    }

    public static StreamGraph singleNoOpStreamGraph() {
        StreamGraph streamGraph = emptyStreamGraph();
        streamGraph.addNode(
                new StreamNode(
                        0, null, null, (StreamOperator<?>) null, "NoOp", NoOpInvokable.class));
        return streamGraph;
    }

    public static StreamGraph buildStreamGraph(StreamNode... nodes) {
        StreamGraph streamGraph = emptyStreamGraph();
        for (StreamNode node : nodes) {
            streamGraph.addNode(node);
        }
        return streamGraph;
    }
}
