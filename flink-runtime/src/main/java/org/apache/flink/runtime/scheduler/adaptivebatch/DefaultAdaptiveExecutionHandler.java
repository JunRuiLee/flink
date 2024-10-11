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

package org.apache.flink.runtime.scheduler.adaptivebatch;

import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResultInfo;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.forwardgroup.StreamNodeForwardGroup;
import org.apache.flink.runtime.jobmaster.event.ExecutionJobVertexFinishedEvent;
import org.apache.flink.runtime.jobmaster.event.JobEvent;
import org.apache.flink.streaming.api.graph.AdaptiveGraphManager;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/** Default implementation of {@link AdaptiveExecutionHandler}. */
public class DefaultAdaptiveExecutionHandler implements AdaptiveExecutionHandler {

    private final Logger log = LoggerFactory.getLogger(DefaultAdaptiveExecutionHandler.class);

    private final List<JobGraphUpdateListener> jobGraphUpdateListeners = new ArrayList<>();

    private final AdaptiveGraphManager adaptiveGraphManager;

    private final StreamGraphOptimizer streamGraphOptimizer;

    public DefaultAdaptiveExecutionHandler(
            ClassLoader userClassloader, StreamGraph streamGraph, Executor serializationExecutor) {
        this.adaptiveGraphManager =
                new AdaptiveGraphManager(
                        userClassloader,
                        streamGraph,
                        serializationExecutor,
                        streamGraph.getJobId());
        this.streamGraphOptimizer = new StreamGraphOptimizer(streamGraph.getJobConfiguration());
    }

    @Override
    public JobGraph getJobGraph() {
        return adaptiveGraphManager.getJobGraph();
    }

    @Override
    public int getPendingOperatorsCount() {
        return adaptiveGraphManager.getPendingOperatorsCount();
    }

    @Override
    public void handleJobEvent(JobEvent jobEvent) {
        try {
            tryAdjustStreamGraph(jobEvent);
        } catch (Exception e) {
            log.error("Failed to handle job event {}.", jobEvent, e);
            throw new RuntimeException(e);
        }
    }

    private void tryAdjustStreamGraph(JobEvent jobEvent) throws Exception {
        if (jobEvent instanceof ExecutionJobVertexFinishedEvent) {
            ExecutionJobVertexFinishedEvent event = (ExecutionJobVertexFinishedEvent) jobEvent;

            tryOptimizeStreamGraph(event);

            List<JobVertex> newlyCreatedJobVertices = tryUpdateJobGraph(event.getVertexId());

            if (!newlyCreatedJobVertices.isEmpty()) {
                notifyJobGraphUpdated(newlyCreatedJobVertices);
            }
        }
    }

    private void tryOptimizeStreamGraph(ExecutionJobVertexFinishedEvent event) throws Exception {
        Map<Integer, List<IntermediateResultInfo>> resultInfoMap =
                event.getResultInfo().entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        entry ->
                                                adaptiveGraphManager.getProducerStreamNodeId(
                                                        entry.getKey()),
                                        entry -> Collections.singletonList(entry.getValue())));

        OperatorsFinished operatorsFinished =
                new OperatorsFinished(
                        adaptiveGraphManager.getStreamNodeIdsByJobVertexId(event.getVertexId()),
                        resultInfoMap);

        streamGraphOptimizer.optimizeStreamGraph(
                operatorsFinished, adaptiveGraphManager.getStreamGraphContext());
    }

    private List<JobVertex> tryUpdateJobGraph(JobVertexID jobVertexId) throws Exception {
        return adaptiveGraphManager.onJobVertexFinished(jobVertexId);
    }

    private void notifyJobGraphUpdated(List<JobVertex> jobVertices) throws Exception {
        for (JobGraphUpdateListener listener : jobGraphUpdateListeners) {
            listener.onNewJobVerticesAdded(jobVertices);
        }
    }

    @Override
    public void registerJobGraphUpdateListener(JobGraphUpdateListener listener) {
        jobGraphUpdateListeners.add(listener);
    }

    @Override
    public int getInitialParallelismByForwardGroup(ExecutionJobVertex jobVertex) {
        int vertexInitialParallelism = jobVertex.getParallelism();
        StreamNodeForwardGroup forwardGroup =
                adaptiveGraphManager.getStreamNodeForwardGroup(jobVertex.getJobVertexId());
        if (!jobVertex.isParallelismDecided()
                && forwardGroup != null
                && forwardGroup.isParallelismDecided()) {
            vertexInitialParallelism = forwardGroup.getParallelism();
            log.info(
                    "Parallelism of JobVertex: {} ({}) is decided to be {} according to forward group's parallelism.",
                    jobVertex.getName(),
                    jobVertex.getJobVertexId(),
                    vertexInitialParallelism);
        }

        return vertexInitialParallelism;
    }

    @Override
    public void updateForwardGroupByNewlyParallelism(
            JobVertexID jobVertexId, int newParallelism, BiConsumer<JobVertexID, Integer> ignored) {
        StreamNodeForwardGroup forwardGroup =
                adaptiveGraphManager.getStreamNodeForwardGroup(jobVertexId);

        if (forwardGroup != null && !forwardGroup.isParallelismDecided()) {
            forwardGroup.setParallelism(newParallelism);

            for (int streamNodeId : forwardGroup.getStartNodeIds()) {
                adaptiveGraphManager.updateStreamNodeParallelism(streamNodeId, newParallelism);
            }
        }
    }
}
