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

package org.apache.flink.runtime.jobmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.testutils.EachCallbackWrapper;
import org.apache.flink.runtime.dispatcher.NoOpStreamGraphListener;
import org.apache.flink.runtime.jobmanager.StreamGraphStore.StreamGraphListener;
import org.apache.flink.runtime.persistence.RetrievableStateStorageHelper;
import org.apache.flink.runtime.state.RetrievableStreamStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.runtime.util.TestingFatalErrorHandlerExtension;
import org.apache.flink.runtime.zookeeper.ZooKeeperExtension;
import org.apache.flink.runtime.zookeeper.ZooKeeperStateHandleStore;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.util.StreamGraphTestUtils;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.Executors;

import org.apache.flink.shaded.curator5.org.apache.curator.framework.CuratorFramework;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.recipes.cache.PathChildrenCache;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import javax.annotation.Nonnull;

import java.util.Collection;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * IT tests for {@link DefaultStreamGraphStore} with all ZooKeeper components(e.g. {@link
 * ZooKeeperStateHandleStore}, {@link ZooKeeperStreamGraphStoreWatcher}, {@link
 * ZooKeeperStreamGraphStoreUtil}).
 */
public class ZooKeeperStreamGraphsStoreITCase extends TestLogger {

    private final ZooKeeperExtension zooKeeperExtension = new ZooKeeperExtension();

    @RegisterExtension
    final EachCallbackWrapper<ZooKeeperExtension> zooKeeperResource =
            new EachCallbackWrapper<>(zooKeeperExtension);

    @RegisterExtension
    final TestingFatalErrorHandlerExtension testingFatalErrorHandlerResource =
            new TestingFatalErrorHandlerExtension();

    private static final RetrievableStateStorageHelper<StreamGraph> localStateStorage =
            jobGraph -> {
                ByteStreamStateHandle byteStreamStateHandle =
                        new ByteStreamStateHandle(
                                String.valueOf(UUID.randomUUID()),
                                InstantiationUtil.serializeObject(jobGraph));
                return new RetrievableStreamStateHandle<>(byteStreamStateHandle);
            };

    @Test
    public void testPutAndRemoveStreamGraph() throws Exception {
        StreamGraphStore streamGraphs =
                createZooKeeperStreamGraphStore("/testPutAndRemoveStreamGraph");

        try {
            StreamGraphStore.StreamGraphListener listener =
                    mock(StreamGraphStore.StreamGraphListener.class);

            streamGraphs.start(listener);

            StreamGraph jobGraph = createStreamGraph(new JobID(), "JobName");

            // Empty state
            assertThat(streamGraphs.getJobIds()).isEmpty();

            // Add initial
            streamGraphs.putStreamGraph(jobGraph);

            // Verify initial job graph
            Collection<JobID> jobIds = streamGraphs.getJobIds();
            assertThat(jobIds).hasSize(1);

            JobID jobId = jobIds.iterator().next();

            verifyStreamGraphs(jobGraph, streamGraphs.recoverStreamGraph(jobId));

            // Update (same ID)
            jobGraph = createStreamGraph(jobGraph.getJobId(), "Updated JobName");
            streamGraphs.putStreamGraph(jobGraph);

            // Verify updated
            jobIds = streamGraphs.getJobIds();
            assertThat(jobIds).hasSize(1);

            jobId = jobIds.iterator().next();

            verifyStreamGraphs(jobGraph, streamGraphs.recoverStreamGraph(jobId));

            // Remove
            streamGraphs.globalCleanupAsync(jobGraph.getJobId(), Executors.directExecutor()).join();

            // Empty state
            assertThat(streamGraphs.getJobIds()).isEmpty();

            // Nothing should have been notified
            verify(listener, atMost(1)).onAddedStreamGraph(any(JobID.class));
            verify(listener, never()).onRemovedStreamGraph(any(JobID.class));

            // Don't fail if called again
            streamGraphs.globalCleanupAsync(jobGraph.getJobId(), Executors.directExecutor()).join();
        } finally {
            streamGraphs.stop();
        }
    }

    @Nonnull
    private StreamGraphStore createZooKeeperStreamGraphStore(String fullPath) throws Exception {
        final CuratorFramework client =
                zooKeeperExtension.getZooKeeperClient(
                        testingFatalErrorHandlerResource.getTestingFatalErrorHandler());
        // Ensure that the job graphs path exists
        client.newNamespaceAwareEnsurePath(fullPath).ensure(client.getZookeeperClient());

        // All operations will have the path as root
        CuratorFramework facade = client.usingNamespace(client.getNamespace() + fullPath);
        final ZooKeeperStateHandleStore<StreamGraph> zooKeeperStateHandleStore =
                new ZooKeeperStateHandleStore<>(facade, localStateStorage);
        return new DefaultStreamGraphStore<>(
                zooKeeperStateHandleStore,
                new ZooKeeperStreamGraphStoreWatcher(new PathChildrenCache(facade, "/", false)),
                ZooKeeperStreamGraphStoreUtil.INSTANCE);
    }

    @Test
    public void testRecoverStreamGraphs() throws Exception {
        StreamGraphStore streamGraphs = createZooKeeperStreamGraphStore("/testRecoverStreamGraphs");

        try {
            StreamGraphListener listener = mock(StreamGraphStore.StreamGraphListener.class);

            streamGraphs.start(listener);

            HashMap<JobID, StreamGraph> expected = new HashMap<>();
            JobID[] jobIds = new JobID[] {new JobID(), new JobID(), new JobID()};

            expected.put(jobIds[0], createStreamGraph(jobIds[0]));
            expected.put(jobIds[1], createStreamGraph(jobIds[1]));
            expected.put(jobIds[2], createStreamGraph(jobIds[2]));

            // Add all
            for (StreamGraph jobGraph : expected.values()) {
                streamGraphs.putStreamGraph(jobGraph);
            }

            Collection<JobID> actual = streamGraphs.getJobIds();

            assertThat(actual).hasSameSizeAs(expected.entrySet());

            for (JobID jobId : actual) {
                StreamGraph jobGraph = streamGraphs.recoverStreamGraph(jobId);
                assertThat(expected).containsKey(jobGraph.getJobId());

                verifyStreamGraphs(expected.get(jobGraph.getJobId()), jobGraph);

                streamGraphs
                        .globalCleanupAsync(jobGraph.getJobId(), Executors.directExecutor())
                        .join();
            }

            // Empty state
            assertThat(streamGraphs.getJobIds()).isEmpty();

            // Nothing should have been notified
            verify(listener, atMost(expected.size())).onAddedStreamGraph(any(JobID.class));
            verify(listener, never()).onRemovedStreamGraph(any(JobID.class));
        } finally {
            streamGraphs.stop();
        }
    }

    @Test
    public void testConcurrentAddStreamGraph() throws Exception {
        StreamGraphStore streamGraphs = null;
        StreamGraphStore otherStreamGraphs = null;

        try {
            streamGraphs = createZooKeeperStreamGraphStore("/testConcurrentAddStreamGraph");

            otherStreamGraphs = createZooKeeperStreamGraphStore("/testConcurrentAddStreamGraph");

            StreamGraph jobGraph = createStreamGraph(new JobID());
            StreamGraph otherStreamGraph = createStreamGraph(new JobID());

            StreamGraphStore.StreamGraphListener listener =
                    mock(StreamGraphStore.StreamGraphListener.class);

            final JobID[] actualOtherJobId = new JobID[1];
            final CountDownLatch sync = new CountDownLatch(1);

            doAnswer(
                            new Answer<Void>() {
                                @Override
                                public Void answer(InvocationOnMock invocation) throws Throwable {
                                    actualOtherJobId[0] = (JobID) invocation.getArguments()[0];
                                    sync.countDown();

                                    return null;
                                }
                            })
                    .when(listener)
                    .onAddedStreamGraph(any(JobID.class));

            // Test
            streamGraphs.start(listener);
            otherStreamGraphs.start(NoOpStreamGraphListener.INSTANCE);

            streamGraphs.putStreamGraph(jobGraph);

            // Everything is cool... not much happening ;)
            verify(listener, never()).onAddedStreamGraph(any(JobID.class));
            verify(listener, never()).onRemovedStreamGraph(any(JobID.class));

            // This bad boy adds the other job graph
            otherStreamGraphs.putStreamGraph(otherStreamGraph);

            // Wait for the cache to call back
            sync.await();

            verify(listener, times(1)).onAddedStreamGraph(any(JobID.class));
            verify(listener, never()).onRemovedStreamGraph(any(JobID.class));

            assertThat(actualOtherJobId[0]).isEqualTo(otherStreamGraph.getJobId());
        } finally {
            if (streamGraphs != null) {
                streamGraphs.stop();
            }

            if (otherStreamGraphs != null) {
                otherStreamGraphs.stop();
            }
        }
    }

    @Test
    public void testUpdateStreamGraphYouDidNotGetOrAdd() throws Exception {
        StreamGraphStore streamGraphs =
                createZooKeeperStreamGraphStore("/testUpdateStreamGraphYouDidNotGetOrAdd");

        StreamGraphStore otherStreamGraphs =
                createZooKeeperStreamGraphStore("/testUpdateStreamGraphYouDidNotGetOrAdd");

        streamGraphs.start(NoOpStreamGraphListener.INSTANCE);
        otherStreamGraphs.start(NoOpStreamGraphListener.INSTANCE);

        StreamGraph jobGraph = createStreamGraph(new JobID());

        streamGraphs.putStreamGraph(jobGraph);

        assertThatExceptionOfType(IllegalStateException.class)
                .isThrownBy(() -> otherStreamGraphs.putStreamGraph(jobGraph));
    }

    /**
     * Tests that we fail with an exception if the job cannot be removed from the
     * ZooKeeperStreamGraphStore.
     *
     * <p>Tests that a close ZooKeeperStreamGraphStore no longer holds any locks.
     */
    @Test
    public void testStreamGraphRemovalFailureAndLockRelease() throws Exception {
        final StreamGraphStore submittedStreamGraphStore =
                createZooKeeperStreamGraphStore("/testConcurrentAddStreamGraph");
        final StreamGraphStore otherSubmittedStreamGraphStore =
                createZooKeeperStreamGraphStore("/testConcurrentAddStreamGraph");

        final TestingStreamGraphListener listener = new TestingStreamGraphListener();
        submittedStreamGraphStore.start(listener);
        otherSubmittedStreamGraphStore.start(listener);

        final StreamGraph jobGraph = StreamGraphTestUtils.emptyStreamGraph();
        submittedStreamGraphStore.putStreamGraph(jobGraph);

        final StreamGraph recoveredStreamGraph =
                otherSubmittedStreamGraphStore.recoverStreamGraph(jobGraph.getJobId());

        assertThat(recoveredStreamGraph).isNotNull();

        assertThatExceptionOfType(Exception.class)
                .as(
                        "It should not be possible to remove the StreamGraph since the first store still has a lock on it.")
                .isThrownBy(
                        () ->
                                otherSubmittedStreamGraphStore
                                        .globalCleanupAsync(
                                                recoveredStreamGraph.getJobId(),
                                                Executors.directExecutor())
                                        .join());

        submittedStreamGraphStore.stop();

        // now we should be able to delete the job graph
        otherSubmittedStreamGraphStore
                .globalCleanupAsync(recoveredStreamGraph.getJobId(), Executors.directExecutor())
                .join();

        assertThat(
                        otherSubmittedStreamGraphStore.recoverStreamGraph(
                                recoveredStreamGraph.getJobId()))
                .isNull();

        otherSubmittedStreamGraphStore.stop();
    }

    // ---------------------------------------------------------------------------------------------

    private StreamGraph createStreamGraph(JobID jobId) {
        return createStreamGraph(jobId, "Test StreamGraph");
    }

    private StreamGraph createStreamGraph(JobID jobId, String jobName) {
        StreamGraph streamGraph = StreamGraphTestUtils.singleNoOpStreamGraph();
        streamGraph.setJobId(jobId);
        streamGraph.setJobName(jobName);

        return streamGraph;
    }

    private void verifyStreamGraphs(StreamGraph expected, StreamGraph actual) {
        assertThat(actual.getJobName()).isEqualTo(expected.getJobName());
        assertThat(actual.getJobId()).isEqualTo(expected.getJobId());
    }
}
