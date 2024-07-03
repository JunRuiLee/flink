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
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.testutils.FlinkMatchers;
import org.apache.flink.runtime.checkpoint.TestingRetrievableStateStorageHelper;
import org.apache.flink.runtime.jobgraph.JobResourceRequirements;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.persistence.IntegerResourceVersion;
import org.apache.flink.runtime.persistence.StateHandleStore;
import org.apache.flink.runtime.persistence.TestingStateHandleStore;
import org.apache.flink.runtime.state.RetrievableStateHandle;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.util.StreamGraphTestUtils;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.Executors;

import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

/**
 * Tests for {@link DefaultStreamGraphStore} with {@link TestingStreamGraphStoreWatcher}, {@link
 * TestingStateHandleStore}, and {@link TestingStreamGraphListener}.
 */
public class DefaultStreamGraphStoreTest extends TestLogger {

    private final StreamGraph testingStreamGraph = StreamGraphTestUtils.emptyStreamGraph();
    private final long timeout = 100L;

    private TestingStateHandleStore.Builder<StreamGraph> builder;
    private TestingRetrievableStateStorageHelper<StreamGraph> streamGraphStorageHelper;
    private TestingStreamGraphStoreWatcher testingStreamGraphStoreWatcher;
    private TestingStreamGraphListener testingStreamGraphListener;

    @Before
    public void setup() {
        builder = TestingStateHandleStore.newBuilder();
        testingStreamGraphStoreWatcher = new TestingStreamGraphStoreWatcher();
        testingStreamGraphListener = new TestingStreamGraphListener();
        streamGraphStorageHelper = new TestingRetrievableStateStorageHelper<>();
    }

    @After
    public void teardown() {
        if (testingStreamGraphStoreWatcher != null) {
            testingStreamGraphStoreWatcher.stop();
        }
    }

    @Test
    public void testRecoverStreamGraph() throws Exception {
        final RetrievableStateHandle<StreamGraph> stateHandle =
                streamGraphStorageHelper.store(testingStreamGraph);
        final TestingStateHandleStore<StreamGraph> stateHandleStore =
                builder.setGetFunction(ignore -> stateHandle).build();

        final StreamGraphStore streamGraphStore = createAndStartStreamGraphStore(stateHandleStore);

        final StreamGraph recoveredStreamGraph =
                streamGraphStore.recoverStreamGraph(testingStreamGraph.getJobId());
        assertThat(recoveredStreamGraph, is(notNullValue()));
        assertThat(recoveredStreamGraph.getJobId(), is(testingStreamGraph.getJobId()));
    }

    @Test
    public void testRecoverStreamGraphWhenNotExist() throws Exception {
        final TestingStateHandleStore<StreamGraph> stateHandleStore =
                builder.setGetFunction(
                                ignore -> {
                                    throw new StateHandleStore.NotExistException(
                                            "Not exist exception.");
                                })
                        .build();

        final StreamGraphStore streamGraphStore = createAndStartStreamGraphStore(stateHandleStore);

        final StreamGraph recoveredStreamGraph =
                streamGraphStore.recoverStreamGraph(testingStreamGraph.getJobId());
        assertThat(recoveredStreamGraph, is(nullValue()));
    }

    @Test
    public void testRecoverStreamGraphFailedShouldReleaseHandle() throws Exception {
        final CompletableFuture<String> releaseFuture = new CompletableFuture<>();
        final FlinkException testException = new FlinkException("Test exception.");
        final TestingStateHandleStore<StreamGraph> stateHandleStore =
                builder.setGetFunction(
                                ignore -> {
                                    throw testException;
                                })
                        .setReleaseConsumer(releaseFuture::complete)
                        .build();

        final StreamGraphStore streamGraphStore = createAndStartStreamGraphStore(stateHandleStore);

        try {
            streamGraphStore.recoverStreamGraph(testingStreamGraph.getJobId());
            fail(
                    "recoverStreamGraph should fail when there is exception in getting the state handle.");
        } catch (Exception ex) {
            assertThat(ex, FlinkMatchers.containsCause(testException));
            String actual = releaseFuture.get(timeout, TimeUnit.MILLISECONDS);
            assertThat(actual, is(testingStreamGraph.getJobId().toString()));
        }
    }

    @Test
    public void testPutStreamGraphWhenNotExist() throws Exception {
        final CompletableFuture<StreamGraph> addFuture = new CompletableFuture<>();
        final TestingStateHandleStore<StreamGraph> stateHandleStore =
                builder.setExistsFunction(ignore -> IntegerResourceVersion.notExisting())
                        .setAddFunction(
                                (ignore, state) -> {
                                    addFuture.complete(state);
                                    return streamGraphStorageHelper.store(state);
                                })
                        .build();

        final StreamGraphStore streamGraphStore = createAndStartStreamGraphStore(stateHandleStore);
        streamGraphStore.putStreamGraph(testingStreamGraph);

        final StreamGraph actual = addFuture.get(timeout, TimeUnit.MILLISECONDS);
        assertThat(actual.getJobId(), is(testingStreamGraph.getJobId()));
    }

    @Test
    public void testPutStreamGraphWhenAlreadyExist() throws Exception {
        final CompletableFuture<Tuple3<String, IntegerResourceVersion, StreamGraph>> replaceFuture =
                new CompletableFuture<>();
        final int resourceVersion = 100;
        final AtomicBoolean alreadyExist = new AtomicBoolean(false);
        final TestingStateHandleStore<StreamGraph> stateHandleStore =
                builder.setExistsFunction(
                                ignore -> {
                                    if (alreadyExist.get()) {
                                        return IntegerResourceVersion.valueOf(resourceVersion);
                                    } else {
                                        alreadyExist.set(true);
                                        return IntegerResourceVersion.notExisting();
                                    }
                                })
                        .setAddFunction((ignore, state) -> streamGraphStorageHelper.store(state))
                        .setReplaceConsumer(replaceFuture::complete)
                        .build();

        final StreamGraphStore streamGraphStore = createAndStartStreamGraphStore(stateHandleStore);
        streamGraphStore.putStreamGraph(testingStreamGraph);
        // Replace
        streamGraphStore.putStreamGraph(testingStreamGraph);

        final Tuple3<String, IntegerResourceVersion, StreamGraph> actual =
                replaceFuture.get(timeout, TimeUnit.MILLISECONDS);
        assertThat(actual.f0, is(testingStreamGraph.getJobId().toString()));
        assertThat(actual.f1, is(IntegerResourceVersion.valueOf(resourceVersion)));
        assertThat(actual.f2.getJobId(), is(testingStreamGraph.getJobId()));
    }

    @Test
    public void testGlobalCleanup() throws Exception {
        final CompletableFuture<JobID> removeFuture = new CompletableFuture<>();
        final TestingStateHandleStore<StreamGraph> stateHandleStore =
                builder.setAddFunction((ignore, state) -> streamGraphStorageHelper.store(state))
                        .setRemoveFunction(name -> removeFuture.complete(JobID.fromHexString(name)))
                        .build();

        final StreamGraphStore streamGraphStore = createAndStartStreamGraphStore(stateHandleStore);

        streamGraphStore.putStreamGraph(testingStreamGraph);
        streamGraphStore
                .globalCleanupAsync(testingStreamGraph.getJobId(), Executors.directExecutor())
                .join();
        final JobID actual = removeFuture.get(timeout, TimeUnit.MILLISECONDS);
        assertThat(actual, is(testingStreamGraph.getJobId()));
    }

    @Test
    public void testGlobalCleanupWithNonExistName() throws Exception {
        final CompletableFuture<JobID> removeFuture = new CompletableFuture<>();
        final TestingStateHandleStore<StreamGraph> stateHandleStore =
                builder.setRemoveFunction(name -> removeFuture.complete(JobID.fromHexString(name)))
                        .build();

        final StreamGraphStore streamGraphStore = createAndStartStreamGraphStore(stateHandleStore);
        streamGraphStore
                .globalCleanupAsync(testingStreamGraph.getJobId(), Executors.directExecutor())
                .join();

        assertThat(removeFuture.isDone(), is(true));
    }

    @Test
    public void testGlobalCleanupFailsIfRemovalReturnsFalse() throws Exception {
        final TestingStateHandleStore<StreamGraph> stateHandleStore =
                builder.setRemoveFunction(name -> false).build();

        final StreamGraphStore streamGraphStore = createAndStartStreamGraphStore(stateHandleStore);
        assertThrows(
                ExecutionException.class,
                () ->
                        streamGraphStore
                                .globalCleanupAsync(
                                        testingStreamGraph.getJobId(), Executors.directExecutor())
                                .get());
    }

    @Test
    public void testGetJobIds() throws Exception {
        final List<JobID> existingJobIds = Arrays.asList(new JobID(0, 0), new JobID(0, 1));
        final TestingStateHandleStore<StreamGraph> stateHandleStore =
                builder.setGetAllHandlesSupplier(
                                () ->
                                        existingJobIds.stream()
                                                .map(AbstractID::toString)
                                                .collect(Collectors.toList()))
                        .build();

        final StreamGraphStore streamGraphStore = createAndStartStreamGraphStore(stateHandleStore);
        final Collection<JobID> jobIds = streamGraphStore.getJobIds();
        assertThat(jobIds, contains(existingJobIds.toArray()));
    }

    @Test
    public void testOnAddedStreamGraphShouldNotProcessKnownStreamGraphs() throws Exception {
        final TestingStateHandleStore<StreamGraph> stateHandleStore =
                builder.setAddFunction((ignore, state) -> streamGraphStorageHelper.store(state))
                        .build();
        final StreamGraphStore streamGraphStore = createAndStartStreamGraphStore(stateHandleStore);
        streamGraphStore.putStreamGraph(testingStreamGraph);

        testingStreamGraphStoreWatcher.addStreamGraph(testingStreamGraph.getJobId());
        assertThat(testingStreamGraphListener.getAddedStreamGraphs().size(), is(0));
    }

    @Test
    public void testOnAddedStreamGraphShouldOnlyProcessUnknownStreamGraphs() throws Exception {
        final RetrievableStateHandle<StreamGraph> stateHandle =
                streamGraphStorageHelper.store(testingStreamGraph);
        final TestingStateHandleStore<StreamGraph> stateHandleStore =
                builder.setGetFunction(ignore -> stateHandle)
                        .setAddFunction((ignore, state) -> streamGraphStorageHelper.store(state))
                        .build();
        final StreamGraphStore streamGraphStore = createAndStartStreamGraphStore(stateHandleStore);
        streamGraphStore.recoverStreamGraph(testingStreamGraph.getJobId());

        // Known recovered job
        testingStreamGraphStoreWatcher.addStreamGraph(testingStreamGraph.getJobId());
        // Unknown job
        final JobID unknownJobId = JobID.generate();
        testingStreamGraphStoreWatcher.addStreamGraph(unknownJobId);
        assertThat(testingStreamGraphListener.getAddedStreamGraphs().size(), is(1));
        assertThat(testingStreamGraphListener.getAddedStreamGraphs(), contains(unknownJobId));
    }

    @Test
    public void testOnRemovedStreamGraphShouldOnlyProcessKnownStreamGraphs() throws Exception {
        final TestingStateHandleStore<StreamGraph> stateHandleStore =
                builder.setAddFunction((ignore, state) -> streamGraphStorageHelper.store(state))
                        .build();
        final StreamGraphStore streamGraphStore = createAndStartStreamGraphStore(stateHandleStore);
        streamGraphStore.putStreamGraph(testingStreamGraph);

        // Unknown job
        testingStreamGraphStoreWatcher.removeStreamGraph(JobID.generate());
        // Known job
        testingStreamGraphStoreWatcher.removeStreamGraph(testingStreamGraph.getJobId());
        assertThat(testingStreamGraphListener.getRemovedStreamGraphs().size(), is(1));
        assertThat(
                testingStreamGraphListener.getRemovedStreamGraphs(),
                contains(testingStreamGraph.getJobId()));
    }

    @Test
    public void testOnRemovedStreamGraphShouldNotProcessUnknownStreamGraphs() throws Exception {
        final TestingStateHandleStore<StreamGraph> stateHandleStore =
                builder.setAddFunction((ignore, state) -> streamGraphStorageHelper.store(state))
                        .build();
        createAndStartStreamGraphStore(stateHandleStore);

        testingStreamGraphStoreWatcher.removeStreamGraph(testingStreamGraph.getJobId());
        assertThat(testingStreamGraphListener.getRemovedStreamGraphs().size(), is(0));
    }

    @Test
    public void testOnAddedStreamGraphIsIgnoredAfterBeingStop() throws Exception {
        final TestingStateHandleStore<StreamGraph> stateHandleStore =
                builder.setAddFunction((ignore, state) -> streamGraphStorageHelper.store(state))
                        .build();
        final StreamGraphStore streamGraphStore = createAndStartStreamGraphStore(stateHandleStore);
        streamGraphStore.stop();

        testingStreamGraphStoreWatcher.addStreamGraph(testingStreamGraph.getJobId());
        assertThat(testingStreamGraphListener.getAddedStreamGraphs().size(), is(0));
    }

    @Test
    public void testOnRemovedStreamGraphIsIgnoredAfterBeingStop() throws Exception {
        final TestingStateHandleStore<StreamGraph> stateHandleStore =
                builder.setAddFunction((ignore, state) -> streamGraphStorageHelper.store(state))
                        .build();
        final StreamGraphStore streamGraphStore = createAndStartStreamGraphStore(stateHandleStore);
        streamGraphStore.putStreamGraph(testingStreamGraph);
        streamGraphStore.stop();

        testingStreamGraphStoreWatcher.removeStreamGraph(testingStreamGraph.getJobId());
        assertThat(testingStreamGraphListener.getRemovedStreamGraphs().size(), is(0));
    }

    @Test
    public void testStoppingStreamGraphStoreShouldReleaseAllHandles() throws Exception {
        final CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        final TestingStateHandleStore<StreamGraph> stateHandleStore =
                builder.setReleaseAllHandlesRunnable(() -> completableFuture.complete(null))
                        .build();
        final StreamGraphStore streamGraphStore = createAndStartStreamGraphStore(stateHandleStore);
        streamGraphStore.stop();

        assertThat(completableFuture.isDone(), is(true));
    }

    @Test
    public void testLocalCleanupShouldReleaseHandle() throws Exception {
        final CompletableFuture<String> releaseFuture = new CompletableFuture<>();
        final TestingStateHandleStore<StreamGraph> stateHandleStore =
                builder.setReleaseConsumer(releaseFuture::complete).build();
        final StreamGraphStore streamGraphStore = createAndStartStreamGraphStore(stateHandleStore);
        streamGraphStore.putStreamGraph(testingStreamGraph);
        streamGraphStore
                .localCleanupAsync(testingStreamGraph.getJobId(), Executors.directExecutor())
                .join();

        final String actual = releaseFuture.get();
        assertThat(actual, is(testingStreamGraph.getJobId().toString()));
    }

    @Test
    public void testRecoverPersistedJobResourceRequirements() throws Exception {
        final Map<String, RetrievableStateHandle<StreamGraph>> handles = new HashMap<>();
        final TestingStateHandleStore<StreamGraph> stateHandleStore =
                builder.setAddFunction(
                                (key, state) -> {
                                    final RetrievableStateHandle<StreamGraph> handle =
                                            streamGraphStorageHelper.store(state);
                                    handles.put(key, handle);
                                    return handle;
                                })
                        .setGetFunction(
                                key -> {
                                    final RetrievableStateHandle<StreamGraph> handle =
                                            handles.get(key);
                                    if (handle != null) {
                                        return handle;
                                    }
                                    throw new StateHandleStore.NotExistException("Does not exist.");
                                })
                        .build();

        final JobResourceRequirements jobResourceRequirements =
                JobResourceRequirements.newBuilder()
                        .setParallelismForJobVertex(new JobVertexID(), 1, 1)
                        .build();

        final StreamGraphStore streamGraphStore = createAndStartStreamGraphStore(stateHandleStore);
        streamGraphStore.putStreamGraph(testingStreamGraph);
        streamGraphStore.putJobResourceRequirements(
                testingStreamGraph.getJobId(), jobResourceRequirements);

        assertStoredRequirementsAre(
                streamGraphStore, testingStreamGraph.getJobId(), jobResourceRequirements);

        final JobResourceRequirements updatedJobResourceRequirements =
                JobResourceRequirements.newBuilder()
                        .setParallelismForJobVertex(new JobVertexID(), 1, 1)
                        .build();

        streamGraphStore.putJobResourceRequirements(
                testingStreamGraph.getJobId(), updatedJobResourceRequirements);

        assertStoredRequirementsAre(
                streamGraphStore, testingStreamGraph.getJobId(), updatedJobResourceRequirements);
    }

    private static void assertStoredRequirementsAre(
            StreamGraphStore streamGraphStore, JobID jobId, JobResourceRequirements expected)
            throws Exception {
        final Optional<JobResourceRequirements> maybeRecovered =
                JobResourceRequirements.readFromJobConfiguration(
                        Objects.requireNonNull(
                                streamGraphStore.recoverStreamGraph(jobId).getJobConfiguration()));
        Assertions.assertThat(maybeRecovered).get().isEqualTo(expected);
    }

    @Test
    public void testPutJobResourceRequirementsOfNonExistentJob() throws Exception {
        final TestingStateHandleStore<StreamGraph> stateHandleStore =
                builder.setGetFunction(
                                ignore -> {
                                    throw new StateHandleStore.NotExistException("Does not exist.");
                                })
                        .build();
        final StreamGraphStore streamGraphStore = createAndStartStreamGraphStore(stateHandleStore);
        assertThrows(
                NoSuchElementException.class,
                () ->
                        streamGraphStore.putJobResourceRequirements(
                                new JobID(), JobResourceRequirements.empty()));
    }

    private StreamGraphStore createAndStartStreamGraphStore(
            TestingStateHandleStore<StreamGraph> stateHandleStore) throws Exception {
        final StreamGraphStore streamGraphStore =
                new DefaultStreamGraphStore<>(
                        stateHandleStore,
                        testingStreamGraphStoreWatcher,
                        new StreamGraphStoreUtil() {
                            @Override
                            public String jobIDToName(JobID jobId) {
                                return jobId.toString();
                            }

                            @Override
                            public JobID nameToJobID(String name) {
                                return JobID.fromHexString(name);
                            }
                        });
        streamGraphStore.start(testingStreamGraphListener);
        return streamGraphStore;
    }
}
