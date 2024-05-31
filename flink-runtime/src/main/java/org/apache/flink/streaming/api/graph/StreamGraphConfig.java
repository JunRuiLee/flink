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

package org.apache.flink.streaming.api.graph;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.transformations.StreamExchangeMode;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.OutputTag;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class StreamGraphConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final String VIRTUAL_PARTITION_NODES = "virtualPartitionNodes";
    private static final String STREAM_NODES = "streamNodes";
    private static final String VIRTUAL_SIDE_OUTPUT_NODES = "virtualSideOutputNodes";
    private static final String EXECUTION_CONFIG = "executionConfig";
    // the following should be removed
    private static final String STATE_BACKEND = "stateBackend";
    private static final String CHECKPOINT_STORAGE = "checkpointStorage";

    private final Configuration config = new Configuration();

    public void serializeStreamNodes(List<StreamNode> toBeSerializedStreamNodes) {
        try {
            InstantiationUtil.writeObjectToConfig(
                    toBeSerializedStreamNodes, this.config, STREAM_NODES);
        } catch (IOException e) {
            throw new RuntimeException("Could not serialize stream nodes", e);
        }
    }

    public void serializeVirtualPartitionNodes(
            Map<Integer, Tuple3<Integer, StreamPartitioner<?>, StreamExchangeMode>>
                    toBeSerializedVirtualPartitionNodes) {
        try {
            InstantiationUtil.writeObjectToConfig(
                    toBeSerializedVirtualPartitionNodes.entrySet().stream()
                            .map(entry -> Tuple2.of(entry.getKey(), entry.getValue()))
                            .collect(Collectors.toList()),
                    this.config,
                    VIRTUAL_PARTITION_NODES);
        } catch (IOException e) {
            throw new RuntimeException("Could not serialize virtual Partition Nodes.", e);
        }
    }

    public void serializeStateBackends(StateBackend stateBackend) {
        try {
            InstantiationUtil.writeObjectToConfig(stateBackend, this.config, STATE_BACKEND);
        } catch (IOException e) {
            throw new RuntimeException("Could not serialize state backend.", e);
        }
    }

    public void serializeCheckpointStorage(CheckpointStorage checkpointStorage) {
        try {
            InstantiationUtil.writeObjectToConfig(
                    checkpointStorage, this.config, CHECKPOINT_STORAGE);
        } catch (IOException e) {
            throw new RuntimeException("Could not serialize checkpoint storage.", e);
        }
    }

    public void serializeVirtualSideOutputNodes(
            Map<Integer, Tuple2<Integer, OutputTag>> virtualSideOutputNodes) {
        try {
            InstantiationUtil.writeObjectToConfig(
                    virtualSideOutputNodes, this.config, VIRTUAL_SIDE_OUTPUT_NODES);
        } catch (IOException e) {
            throw new RuntimeException("Could not serialize virtualSideOutputNodes.", e);
        }
    }

    public void serializeExecutionConfig(ExecutionConfig toBeSerializeExecutionConfig) {
        try {
            InstantiationUtil.writeObjectToConfig(
                    toBeSerializeExecutionConfig, this.config, EXECUTION_CONFIG);
        } catch (IOException e) {
            throw new RuntimeException("Could not serialize execution config.", e);
        }
    }

    public ExecutionConfig getExecutionConfig(ClassLoader cl) {
        try {
            return InstantiationUtil.readObjectFromConfig(this.config, EXECUTION_CONFIG, cl);
        } catch (Exception e) {
            throw new RuntimeException("Could not deserialize serializer config.");
        }
    }

    public List<StreamNode> getStreamNodes(ClassLoader cl) {
        try {
            return InstantiationUtil.readObjectFromConfig(this.config, STREAM_NODES, cl);
        } catch (Exception e) {
            throw new RuntimeException("Could not deserialize stream nodes.");
        }
    }

    public StateBackend getStateBackend(ClassLoader cl) {
        try {
            return InstantiationUtil.readObjectFromConfig(this.config, STATE_BACKEND, cl);
        } catch (Exception e) {
            throw new RuntimeException("Could not deserialize state backend.");
        }
    }

    public CheckpointStorage getCheckpointStorage(ClassLoader cl) {
        try {
            return InstantiationUtil.readObjectFromConfig(this.config, CHECKPOINT_STORAGE, cl);
        } catch (Exception e) {
            throw new RuntimeException("Could not deserialize checkpoint storage.");
        }
    }

    public Map<Integer, Tuple2<Integer, OutputTag>> getVirtualSideOutputNodes(ClassLoader cl) {
        try {
            return InstantiationUtil.readObjectFromConfig(
                    this.config, VIRTUAL_SIDE_OUTPUT_NODES, cl);
        } catch (Exception e) {
            throw new RuntimeException("Could not deserialize checkpoint storage.");
        }
    }

    public Map<Integer, Tuple3<Integer, StreamPartitioner<?>, StreamExchangeMode>>
            getVirtualPartitionNodes(ClassLoader cl) {
        try {
            List<Tuple2<Integer, Tuple3<Integer, StreamPartitioner<?>, StreamExchangeMode>>> list =
                    InstantiationUtil.readObjectFromConfig(
                            this.config, VIRTUAL_PARTITION_NODES, cl);
            Map<Integer, Tuple3<Integer, StreamPartitioner<?>, StreamExchangeMode>>
                    virtualPartitionNodes = new HashMap<>();
            list.forEach(tuple2 -> virtualPartitionNodes.put(tuple2.f0, tuple2.f1));

            return virtualPartitionNodes;
        } catch (Exception e) {
            throw new RuntimeException("Could not deserialize stream nodes.");
        }
    }

    public int getNumberOfVertices() {
        return config.toMap().size();
    }
}
