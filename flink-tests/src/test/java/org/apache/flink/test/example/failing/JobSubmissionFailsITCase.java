/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.test.example.failing;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.util.StreamGraphTestUtils;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Test;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.function.Predicate;

import static org.apache.flink.test.util.TestUtils.submitJobAndWaitForResult;
import static org.junit.Assert.fail;

/** Tests for failing job submissions. */
public class JobSubmissionFailsITCase extends TestLogger {

    private static final int NUM_TM = 2;
    private static final int NUM_SLOTS = 20;

    @ClassRule
    public static final MiniClusterWithClientResource MINI_CLUSTER_RESOURCE =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(getConfiguration())
                            .setNumberTaskManagers(NUM_TM)
                            .setNumberSlotsPerTaskManager(NUM_SLOTS / NUM_TM)
                            .build());

    private static Configuration getConfiguration() {
        Configuration config = new Configuration();
        config.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.parse("4m"));

        // to accommodate for 10 netty arenas (NUM_SLOTS / NUM_TM) x 16Mb
        // (NettyBufferPool.ARENA_SIZE)
        config.set(TaskManagerOptions.NETWORK_MEMORY_MIN, MemorySize.parse("256m"));

        return config;
    }

    private static StreamGraph getWorkingStreamGraph() {
        return StreamGraphTestUtils.singleNoOpStreamGraph();
    }

    // --------------------------------------------------------------------------------------------

    @Test
    public void testExceptionInInitializeOnMaster() throws Exception {
        final StreamNode failingStreamNode = new StreamNode(0, "node", NoOpInvokable.class);
        failingStreamNode.setInputFormat(
                new InputFormat<Object, InputSplit>() {
                    @Override
                    public void configure(Configuration parameters) {
                        throw new RuntimeException("Test exception.");
                    }

                    @Override
                    public BaseStatistics getStatistics(BaseStatistics cachedStatistics)
                            throws IOException {
                        return null;
                    }

                    @Override
                    public InputSplit[] createInputSplits(int minNumSplits) throws IOException {
                        return new InputSplit[0];
                    }

                    @Override
                    public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
                        return null;
                    }

                    @Override
                    public void open(InputSplit split) throws IOException {}

                    @Override
                    public boolean reachedEnd() throws IOException {
                        return false;
                    }

                    @Override
                    public Object nextRecord(Object reuse) throws IOException {
                        return null;
                    }

                    @Override
                    public void close() throws IOException {}
                });

        final StreamGraph failingStreamGraph =
                StreamGraphTestUtils.buildStreamGraph(failingStreamNode);
        runJobSubmissionTest(
                failingStreamGraph,
                e ->
                        ExceptionUtils.findThrowable(
                                        e,
                                        candidate ->
                                                "Test exception.".equals(candidate.getMessage()))
                                .isPresent());
    }

    @Test
    public void testSubmitEmptyStreamGraph() throws Exception {
        final StreamGraph streamGraph = StreamGraphTestUtils.emptyStreamGraph();
        runJobSubmissionTest(
                streamGraph,
                e ->
                        ExceptionUtils.findThrowable(
                                        e,
                                        throwable ->
                                                throwable.getMessage() != null
                                                        && throwable.getMessage().contains("empty"))
                                .isPresent());
    }

    @Test
    public void testMissingJarBlob() throws Exception {
        final StreamGraph streamGraph = getStreamGraphWithMissingBlobKey();
        runJobSubmissionTest(
                streamGraph, e -> ExceptionUtils.findThrowable(e, IOException.class).isPresent());
    }

    private void runJobSubmissionTest(
            StreamGraph streamGraph, Predicate<Exception> failurePredicate) throws Exception {
        ClusterClient<?> client = MINI_CLUSTER_RESOURCE.getClusterClient();

        try {
            submitJobAndWaitForResult(client, streamGraph, getClass().getClassLoader());
            fail("Job submission should have thrown an exception.");
        } catch (Exception e) {
            if (!failurePredicate.test(e)) {
                throw e;
            }
        }

        submitJobAndWaitForResult(client, getWorkingStreamGraph(), getClass().getClassLoader());
    }

    @Nonnull
    private static StreamGraph getStreamGraphWithMissingBlobKey() {
        final StreamGraph streamGraph = getWorkingStreamGraph();
        streamGraph.addUserJarBlobKey(new PermanentBlobKey());
        return streamGraph;
    }
}
