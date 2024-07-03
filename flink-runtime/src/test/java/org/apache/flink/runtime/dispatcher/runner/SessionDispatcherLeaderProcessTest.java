/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.dispatcher.runner;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.testutils.FlinkAssertions;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.client.DuplicateJobSubmissionException;
import org.apache.flink.runtime.client.JobSubmissionException;
import org.apache.flink.runtime.highavailability.JobResultStore;
import org.apache.flink.runtime.jobmanager.StreamGraphStore;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.testutils.TestingJobResultStore;
import org.apache.flink.runtime.testutils.TestingStreamGraphStore;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.runtime.webmonitor.TestingDispatcherGateway;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.util.StreamGraphTestUtils;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLoggerExtension;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.ThrowingConsumer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.flink.core.testutils.FlinkAssertions.STREAM_THROWABLE;
import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.apache.flink.core.testutils.FlinkAssertions.assertThatFuture;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link SessionDispatcherLeaderProcess}. */
@ExtendWith(TestLoggerExtension.class)
class SessionDispatcherLeaderProcessTest {

    private static final StreamGraph STREAM_GRAPH = StreamGraphTestUtils.emptyStreamGraph();

    private static ExecutorService ioExecutor;

    private final UUID leaderSessionId = UUID.randomUUID();

    private TestingFatalErrorHandler fatalErrorHandler;

    private StreamGraphStore streamGraphStore;
    private JobResultStore jobResultStore;

    private AbstractDispatcherLeaderProcess.DispatcherGatewayServiceFactory
            dispatcherServiceFactory;

    @BeforeAll
    static void setupClass() {
        ioExecutor = Executors.newSingleThreadExecutor();
    }

    @BeforeEach
    void setup() {
        fatalErrorHandler = new TestingFatalErrorHandler();
        streamGraphStore = TestingStreamGraphStore.newBuilder().build();
        jobResultStore = TestingJobResultStore.builder().build();
        dispatcherServiceFactory =
                createFactoryBasedOnGenericSupplier(
                        () -> TestingDispatcherGatewayService.newBuilder().build());
    }

    @AfterEach
    void teardown() throws Exception {
        if (fatalErrorHandler != null) {
            fatalErrorHandler.rethrowError();
            fatalErrorHandler = null;
        }
    }

    @AfterAll
    static void teardownClass() {
        if (ioExecutor != null) {
            ExecutorUtils.gracefulShutdown(5L, TimeUnit.SECONDS, ioExecutor);
        }
    }

    @Test
    void start_afterClose_doesNotHaveAnEffect() throws Exception {
        final SessionDispatcherLeaderProcess dispatcherLeaderProcess =
                createDispatcherLeaderProcess();

        dispatcherLeaderProcess.close();
        dispatcherLeaderProcess.start();

        assertThat(dispatcherLeaderProcess.getState())
                .isEqualTo(SessionDispatcherLeaderProcess.State.STOPPED);
    }

    @Test
    void testStartTriggeringDispatcherServiceCreation() throws Exception {
        dispatcherServiceFactory =
                createFactoryBasedOnGenericSupplier(
                        () -> TestingDispatcherGatewayService.newBuilder().build());

        try (final SessionDispatcherLeaderProcess dispatcherLeaderProcess =
                createDispatcherLeaderProcess()) {
            dispatcherLeaderProcess.start();
            assertThat(dispatcherLeaderProcess.getState())
                    .isEqualTo(SessionDispatcherLeaderProcess.State.RUNNING);
        }
    }

    @Test
    void testRecoveryWithStreamGraphButNoDirtyJobResult() throws Exception {
        testJobRecovery(
                Collections.singleton(STREAM_GRAPH),
                Collections.emptySet(),
                actualRecoveredStreamGraphs ->
                        assertThat(actualRecoveredStreamGraphs)
                                .singleElement()
                                .isEqualTo(STREAM_GRAPH),
                actualRecoveredDirtyJobResults ->
                        assertThat(actualRecoveredDirtyJobResults).isEmpty());
    }

    @Test
    void testRecoveryWithStreamGraphAndMatchingDirtyJobResult() throws Exception {
        final JobResult matchingDirtyJobResult =
                TestingJobResultStore.createSuccessfulJobResult(STREAM_GRAPH.getJobId());

        testJobRecovery(
                Collections.singleton(STREAM_GRAPH),
                Collections.singleton(matchingDirtyJobResult),
                actualRecoveredStreamGraphs -> assertThat(actualRecoveredStreamGraphs).isEmpty(),
                actualRecoveredDirtyJobResults ->
                        assertThat(actualRecoveredDirtyJobResults)
                                .singleElement()
                                .isEqualTo(matchingDirtyJobResult));
    }

    @Test
    void testRecoveryWithMultipleStreamGraphsAndOneMatchingDirtyJobResult() throws Exception {
        final JobResult matchingDirtyJobResult =
                TestingJobResultStore.createSuccessfulJobResult(STREAM_GRAPH.getJobId());
        final StreamGraph otherStreamGraph = StreamGraphTestUtils.emptyStreamGraph();

        testJobRecovery(
                Arrays.asList(otherStreamGraph, STREAM_GRAPH),
                Collections.singleton(matchingDirtyJobResult),
                actualRecoveredStreamGraphs ->
                        assertThat(actualRecoveredStreamGraphs)
                                .singleElement()
                                .isEqualTo(otherStreamGraph),
                actualRecoveredDirtyJobResults ->
                        assertThat(actualRecoveredDirtyJobResults)
                                .singleElement()
                                .isEqualTo(matchingDirtyJobResult));
    }

    @Test
    void testRecoveryWithoutStreamGraphButDirtyJobResult() throws Exception {
        final JobResult dirtyJobResult =
                TestingJobResultStore.createSuccessfulJobResult(new JobID());

        testJobRecovery(
                Collections.emptyList(),
                Collections.singleton(dirtyJobResult),
                actualRecoveredStreamGraphs -> assertThat(actualRecoveredStreamGraphs).isEmpty(),
                actualRecoveredDirtyJobResults ->
                        assertThat(actualRecoveredDirtyJobResults)
                                .singleElement()
                                .isEqualTo(dirtyJobResult));
    }

    private void testJobRecovery(
            Collection<StreamGraph> streamGraphsToRecover,
            Set<JobResult> dirtyJobResults,
            Consumer<Collection<StreamGraph>> recoveredStreamGraphAssertion,
            Consumer<Collection<JobResult>> recoveredDirtyJobResultAssertion)
            throws Exception {
        streamGraphStore =
                TestingStreamGraphStore.newBuilder()
                        .setInitialStreamGraphs(streamGraphsToRecover)
                        .build();

        jobResultStore =
                TestingJobResultStore.builder()
                        .withGetDirtyResultsSupplier(() -> dirtyJobResults)
                        .build();

        final CompletableFuture<Collection<StreamGraph>> recoveredStreamGraphsFuture =
                new CompletableFuture<>();
        final CompletableFuture<Collection<JobResult>> recoveredDirtyJobResultsFuture =
                new CompletableFuture<>();
        dispatcherServiceFactory =
                (ignoredDispatcherId,
                        recoveredJobs,
                        recoveredDirtyJobResults,
                        ignoredStreamGraphWriter,
                        ignoredJobResultStore) -> {
                    recoveredStreamGraphsFuture.complete(recoveredJobs);
                    recoveredDirtyJobResultsFuture.complete(recoveredDirtyJobResults);
                    return TestingDispatcherGatewayService.newBuilder().build();
                };

        try (final SessionDispatcherLeaderProcess dispatcherLeaderProcess =
                createDispatcherLeaderProcess()) {
            dispatcherLeaderProcess.start();

            recoveredStreamGraphAssertion.accept(recoveredStreamGraphsFuture.get());
            recoveredDirtyJobResultAssertion.accept(recoveredDirtyJobResultsFuture.get());
        }
    }

    @Test
    void testRecoveryWhileStreamGraphRecoveryIsScheduledConcurrently() throws Exception {
        final JobResult dirtyJobResult =
                TestingJobResultStore.createSuccessfulJobResult(new JobID());

        OneShotLatch recoveryInitiatedLatch = new OneShotLatch();
        OneShotLatch streamGraphAddedLatch = new OneShotLatch();

        streamGraphStore =
                TestingStreamGraphStore.newBuilder()
                        // mimic behavior when recovering a StreamGraph that is marked for deletion
                        .setRecoverStreamGraphFunction((jobId, jobs) -> null)
                        .build();

        jobResultStore =
                TestingJobResultStore.builder()
                        .withGetDirtyResultsSupplier(
                                () -> {
                                    recoveryInitiatedLatch.trigger();
                                    try {
                                        streamGraphAddedLatch.await();
                                    } catch (InterruptedException e) {
                                        Thread.currentThread().interrupt();
                                    }
                                    return Collections.singleton(dirtyJobResult);
                                })
                        .build();

        final CompletableFuture<Collection<StreamGraph>> recoveredStreamGraphsFuture =
                new CompletableFuture<>();
        final CompletableFuture<Collection<JobResult>> recoveredDirtyJobResultsFuture =
                new CompletableFuture<>();
        dispatcherServiceFactory =
                (ignoredDispatcherId,
                        recoveredJobs,
                        recoveredDirtyJobResults,
                        ignoredStreamGraphWriter,
                        ignoredJobResultStore) -> {
                    recoveredStreamGraphsFuture.complete(recoveredJobs);
                    recoveredDirtyJobResultsFuture.complete(recoveredDirtyJobResults);
                    return TestingDispatcherGatewayService.newBuilder().build();
                };

        try (final SessionDispatcherLeaderProcess dispatcherLeaderProcess =
                createDispatcherLeaderProcess()) {
            dispatcherLeaderProcess.start();

            // start returns without the initial recovery being completed
            // mimic ZK message about an added jobgraph while the recovery is ongoing
            recoveryInitiatedLatch.await();
            dispatcherLeaderProcess.onAddedStreamGraph(dirtyJobResult.getJobId());
            streamGraphAddedLatch.trigger();

            assertThatFuture(recoveredStreamGraphsFuture)
                    .eventuallySucceeds()
                    .satisfies(recoverStreamGraphs -> assertThat(recoverStreamGraphs).isEmpty());
            assertThatFuture(recoveredDirtyJobResultsFuture)
                    .eventuallySucceeds()
                    .satisfies(
                            recoveredDirtyJobResults ->
                                    assertThat(recoveredDirtyJobResults)
                                            .containsExactly(dirtyJobResult));
        }
    }

    @Test
    void closeAsync_stopsStreamGraphStoreAndDispatcher() throws Exception {
        final CompletableFuture<Void> streamGraphStopFuture = new CompletableFuture<>();
        streamGraphStore =
                TestingStreamGraphStore.newBuilder()
                        .setStopRunnable(() -> streamGraphStopFuture.complete(null))
                        .build();

        final CompletableFuture<Void> dispatcherServiceTerminationFuture =
                new CompletableFuture<>();
        dispatcherServiceFactory =
                createFactoryBasedOnGenericSupplier(
                        () ->
                                TestingDispatcherGatewayService.newBuilder()
                                        .setTerminationFuture(dispatcherServiceTerminationFuture)
                                        .withManualTerminationFutureCompletion()
                                        .build());

        try (final SessionDispatcherLeaderProcess dispatcherLeaderProcess =
                createDispatcherLeaderProcess()) {
            dispatcherLeaderProcess.start();

            // wait for the creation of the DispatcherGatewayService
            dispatcherLeaderProcess.getDispatcherGateway().get();

            final CompletableFuture<Void> terminationFuture = dispatcherLeaderProcess.closeAsync();

            assertThat(streamGraphStopFuture).isNotDone();
            assertThat(terminationFuture).isNotDone();

            dispatcherServiceTerminationFuture.complete(null);

            // verify that we shut down the StreamGraphStore
            streamGraphStopFuture.get();

            // verify that we completed the dispatcher leader process shut down
            terminationFuture.get();
        }
    }

    @Test
    void unexpectedDispatcherServiceTerminationWhileRunning_callsFatalErrorHandler() {
        final CompletableFuture<Void> terminationFuture = new CompletableFuture<>();
        dispatcherServiceFactory =
                createFactoryBasedOnGenericSupplier(
                        () ->
                                TestingDispatcherGatewayService.newBuilder()
                                        .setTerminationFuture(terminationFuture)
                                        .build());

        final SessionDispatcherLeaderProcess dispatcherLeaderProcess =
                createDispatcherLeaderProcess();
        dispatcherLeaderProcess.start();

        final FlinkException expectedFailure = new FlinkException("Expected test failure.");
        terminationFuture.completeExceptionally(expectedFailure);

        final Throwable error = fatalErrorHandler.getErrorFuture().join();
        assertThat(error).rootCause().isEqualTo(expectedFailure);

        fatalErrorHandler.clearError();
    }

    @Test
    void unexpectedDispatcherServiceTerminationWhileNotRunning_doesNotCallFatalErrorHandler() {
        final CompletableFuture<Void> terminationFuture = new CompletableFuture<>();
        dispatcherServiceFactory =
                createFactoryBasedOnGenericSupplier(
                        () ->
                                TestingDispatcherGatewayService.newBuilder()
                                        .setTerminationFuture(terminationFuture)
                                        .withManualTerminationFutureCompletion()
                                        .build());
        final SessionDispatcherLeaderProcess dispatcherLeaderProcess =
                createDispatcherLeaderProcess();
        dispatcherLeaderProcess.start();

        dispatcherLeaderProcess.closeAsync();

        final FlinkException expectedFailure = new FlinkException("Expected test failure.");
        terminationFuture.completeExceptionally(expectedFailure);

        assertThatThrownBy(() -> fatalErrorHandler.getErrorFuture().get(10, TimeUnit.MILLISECONDS))
                .isInstanceOf(TimeoutException.class);
    }

    @Test
    void confirmLeaderSessionFuture_completesAfterDispatcherServiceHasBeenStarted()
            throws Exception {
        final OneShotLatch createDispatcherServiceLatch = new OneShotLatch();
        final String dispatcherAddress = "myAddress";
        final TestingDispatcherGateway dispatcherGateway =
                TestingDispatcherGateway.newBuilder().setAddress(dispatcherAddress).build();

        dispatcherServiceFactory =
                createFactoryBasedOnGenericSupplier(
                        () -> {
                            try {
                                createDispatcherServiceLatch.await();
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                            return TestingDispatcherGatewayService.newBuilder()
                                    .setDispatcherGateway(dispatcherGateway)
                                    .build();
                        });

        try (final SessionDispatcherLeaderProcess dispatcherLeaderProcess =
                createDispatcherLeaderProcess()) {
            final CompletableFuture<String> confirmLeaderSessionFuture =
                    dispatcherLeaderProcess.getLeaderAddressFuture();

            dispatcherLeaderProcess.start();

            assertThat(confirmLeaderSessionFuture).isNotDone();

            createDispatcherServiceLatch.trigger();

            assertThatFuture(confirmLeaderSessionFuture)
                    .eventuallySucceeds()
                    .isEqualTo(dispatcherAddress);
        }
    }

    @Test
    void closeAsync_duringJobRecovery_preventsDispatcherServiceCreation() throws Exception {
        final OneShotLatch jobRecoveryStartedLatch = new OneShotLatch();
        final OneShotLatch completeJobRecoveryLatch = new OneShotLatch();
        final OneShotLatch createDispatcherServiceLatch = new OneShotLatch();

        this.streamGraphStore =
                TestingStreamGraphStore.newBuilder()
                        .setJobIdsFunction(
                                storedJobs -> {
                                    jobRecoveryStartedLatch.trigger();
                                    completeJobRecoveryLatch.await();
                                    return storedJobs;
                                })
                        .build();

        this.dispatcherServiceFactory =
                createFactoryBasedOnGenericSupplier(
                        () -> {
                            createDispatcherServiceLatch.trigger();
                            return TestingDispatcherGatewayService.newBuilder().build();
                        });

        try (final SessionDispatcherLeaderProcess dispatcherLeaderProcess =
                createDispatcherLeaderProcess()) {
            dispatcherLeaderProcess.start();

            jobRecoveryStartedLatch.await();

            dispatcherLeaderProcess.closeAsync();

            completeJobRecoveryLatch.trigger();

            assertThatThrownBy(
                            () -> createDispatcherServiceLatch.await(10L, TimeUnit.MILLISECONDS),
                            "No dispatcher service should be created after the process has been stopped.")
                    .isInstanceOf(TimeoutException.class);
        }
    }

    @Test
    void onRemovedStreamGraph_terminatesRunningJob() throws Exception {
        streamGraphStore =
                TestingStreamGraphStore.newBuilder()
                        .setInitialStreamGraphs(Collections.singleton(STREAM_GRAPH))
                        .build();

        final CompletableFuture<JobID> terminateJobFuture = new CompletableFuture<>();
        final TestingDispatcherGatewayService testingDispatcherService =
                TestingDispatcherGatewayService.newBuilder()
                        .setOnRemovedStreamGraphFunction(
                                jobID -> {
                                    terminateJobFuture.complete(jobID);
                                    return FutureUtils.completedVoidFuture();
                                })
                        .build();

        dispatcherServiceFactory =
                createFactoryBasedOnGenericSupplier(() -> testingDispatcherService);

        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        try (final SessionDispatcherLeaderProcess dispatcherLeaderProcess =
                createDispatcherLeaderProcess()) {
            dispatcherLeaderProcess.start();

            // wait for the dispatcher process to be created
            dispatcherLeaderProcess.getDispatcherGateway().get();

            // now remove the Job from the StreamGraphStore and notify the dispatcher service
            streamGraphStore.globalCleanupAsync(STREAM_GRAPH.getJobId(), executorService).join();
            dispatcherLeaderProcess.onRemovedStreamGraph(STREAM_GRAPH.getJobId());

            assertThat(terminateJobFuture.get()).isEqualTo(STREAM_GRAPH.getJobId());
        } finally {
            assertThat(executorService.shutdownNow()).isEmpty();
        }
    }

    @Test
    void onRemovedStreamGraph_failingRemovalCall_failsFatally() throws Exception {
        final FlinkException testException = new FlinkException("Test exception");

        final TestingDispatcherGatewayService testingDispatcherService =
                TestingDispatcherGatewayService.newBuilder()
                        .setOnRemovedStreamGraphFunction(
                                jobID -> FutureUtils.completedExceptionally(testException))
                        .build();

        dispatcherServiceFactory =
                createFactoryBasedOnGenericSupplier(() -> testingDispatcherService);

        try (final SessionDispatcherLeaderProcess dispatcherLeaderProcess =
                createDispatcherLeaderProcess()) {
            dispatcherLeaderProcess.start();

            // wait for the dispatcher process to be created
            dispatcherLeaderProcess.getDispatcherGateway().get();

            // now notify the dispatcher service
            dispatcherLeaderProcess.onRemovedStreamGraph(STREAM_GRAPH.getJobId());

            final Throwable fatalError = fatalErrorHandler.getErrorFuture().join();

            assertThat(fatalError).hasCause(testException);

            fatalErrorHandler.clearError();
        }
    }

    @Test
    void onAddedStreamGraph_submitsRecoveredJob() throws Exception {
        final CompletableFuture<StreamGraph> submittedJobFuture = new CompletableFuture<>();
        final TestingDispatcherGateway testingDispatcherGateway =
                TestingDispatcherGateway.newBuilder()
                        .setSubmitFunction(
                                submittedJob -> {
                                    submittedJobFuture.complete(submittedJob);
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .build();

        dispatcherServiceFactory =
                createFactoryBasedOnGenericSupplier(
                        () ->
                                TestingDispatcherGatewayService.newBuilder()
                                        .setDispatcherGateway(testingDispatcherGateway)
                                        .build());

        try (final SessionDispatcherLeaderProcess dispatcherLeaderProcess =
                createDispatcherLeaderProcess()) {
            dispatcherLeaderProcess.start();

            // wait first for the dispatcher service to be created
            dispatcherLeaderProcess.getDispatcherGateway().get();

            streamGraphStore.putStreamGraph(STREAM_GRAPH);
            dispatcherLeaderProcess.onAddedStreamGraph(STREAM_GRAPH.getJobId());

            final StreamGraph submittedStreamGraph = submittedJobFuture.get();

            assertThat(submittedStreamGraph.getJobId()).isEqualTo(STREAM_GRAPH.getJobId());
        }
    }

    @Test
    void onAddedStreamGraph_ifNotRunning_isBeingIgnored() throws Exception {
        final CompletableFuture<JobID> recoveredJobFuture = new CompletableFuture<>();
        streamGraphStore =
                TestingStreamGraphStore.newBuilder()
                        .setRecoverStreamGraphFunction(
                                (jobId, streamGraphs) -> {
                                    recoveredJobFuture.complete(jobId);
                                    return streamGraphs.get(jobId);
                                })
                        .build();

        try (final SessionDispatcherLeaderProcess dispatcherLeaderProcess =
                createDispatcherLeaderProcess()) {
            dispatcherLeaderProcess.start();

            // wait until the process has started the dispatcher
            dispatcherLeaderProcess.getDispatcherGateway().get();

            // now add the job graph
            streamGraphStore.putStreamGraph(STREAM_GRAPH);

            dispatcherLeaderProcess.closeAsync();

            dispatcherLeaderProcess.onAddedStreamGraph(STREAM_GRAPH.getJobId());

            assertThatThrownBy(
                            () -> recoveredJobFuture.get(10L, TimeUnit.MILLISECONDS),
                            "onAddedStreamGraph should be ignored if the leader process is not running.")
                    .isInstanceOf(TimeoutException.class);
        }
    }

    @Test
    void onAddedStreamGraph_failingRecovery_propagatesTheFailure() throws Exception {
        final FlinkException expectedFailure = new FlinkException("Expected failure");
        streamGraphStore =
                TestingStreamGraphStore.newBuilder()
                        .setRecoverStreamGraphFunction(
                                (ignoredA, ignoredB) -> {
                                    throw expectedFailure;
                                })
                        .build();

        try (final SessionDispatcherLeaderProcess dispatcherLeaderProcess =
                createDispatcherLeaderProcess()) {
            dispatcherLeaderProcess.start();

            // wait first for the dispatcher service to be created
            dispatcherLeaderProcess.getDispatcherGateway().get();

            streamGraphStore.putStreamGraph(STREAM_GRAPH);
            dispatcherLeaderProcess.onAddedStreamGraph(STREAM_GRAPH.getJobId());

            assertThatFuture(fatalErrorHandler.getErrorFuture())
                    .eventuallySucceeds()
                    .extracting(FlinkAssertions::chainOfCauses, STREAM_THROWABLE)
                    .contains(expectedFailure);

            assertThat(dispatcherLeaderProcess.getState())
                    .isEqualTo(SessionDispatcherLeaderProcess.State.STOPPED);

            fatalErrorHandler.clearError();
        }
    }

    @Test
    void recoverJobs_withRecoveryFailure_failsFatally() throws Exception {
        final FlinkException testException = new FlinkException("Test exception");
        streamGraphStore =
                TestingStreamGraphStore.newBuilder()
                        .setRecoverStreamGraphFunction(
                                (ignoredA, ignoredB) -> {
                                    throw testException;
                                })
                        .setInitialStreamGraphs(Collections.singleton(STREAM_GRAPH))
                        .build();

        runJobRecoveryFailureTest(testException);
    }

    @Test
    void recoverJobs_withJobIdRecoveryFailure_failsFatally() throws Exception {
        final FlinkException testException = new FlinkException("Test exception");
        streamGraphStore =
                TestingStreamGraphStore.newBuilder()
                        .setJobIdsFunction(
                                ignored -> {
                                    throw testException;
                                })
                        .build();

        runJobRecoveryFailureTest(testException);
    }

    private void runJobRecoveryFailureTest(FlinkException testException) throws Exception {
        try (final SessionDispatcherLeaderProcess dispatcherLeaderProcess =
                createDispatcherLeaderProcess()) {
            dispatcherLeaderProcess.start();

            // we expect that a fatal error occurred
            assertThatFuture(fatalErrorHandler.getErrorFuture())
                    .eventuallySucceeds()
                    .satisfies(
                            error ->
                                    assertThat(error)
                                            .satisfies(
                                                    anyCauseMatches(
                                                            testException.getClass(),
                                                            testException.getMessage())));

            fatalErrorHandler.clearError();
        }
    }

    @Test
    void onAddedStreamGraph_failingRecoveredJobSubmission_failsFatally() throws Exception {
        final TestingDispatcherGateway dispatcherGateway =
                TestingDispatcherGateway.newBuilder()
                        .setSubmitFunction(
                                streamGraph ->
                                        FutureUtils.completedExceptionally(
                                                new JobSubmissionException(
                                                        streamGraph.getJobId(), "test exception")))
                        .build();

        runOnAddedStreamGraphTest(
                dispatcherGateway, this::verifyOnAddedStreamGraphResultFailsFatally);
    }

    private void verifyOnAddedStreamGraphResultFailsFatally(
            TestingFatalErrorHandler fatalErrorHandler) {
        final Throwable actualCause = fatalErrorHandler.getErrorFuture().join();

        assertThat(actualCause)
                .extracting(FlinkAssertions::chainOfCauses, FlinkAssertions.STREAM_THROWABLE)
                .hasAtLeastOneElementOfType(JobSubmissionException.class);

        fatalErrorHandler.clearError();
    }

    @Test
    void onAddedStreamGraph_duplicateJobSubmissionDueToFalsePositive_willBeIgnored()
            throws Exception {
        final TestingDispatcherGateway dispatcherGateway =
                TestingDispatcherGateway.newBuilder()
                        .setSubmitFunction(
                                streamGraph ->
                                        FutureUtils.completedExceptionally(
                                                DuplicateJobSubmissionException.of(
                                                        streamGraph.getJobId())))
                        .build();

        runOnAddedStreamGraphTest(
                dispatcherGateway, this::verifyOnAddedStreamGraphResultDidNotFail);
    }

    private void runOnAddedStreamGraphTest(
            TestingDispatcherGateway dispatcherGateway,
            ThrowingConsumer<TestingFatalErrorHandler, Exception> verificationLogic)
            throws Exception {
        streamGraphStore =
                TestingStreamGraphStore.newBuilder()
                        .setInitialStreamGraphs(Collections.singleton(STREAM_GRAPH))
                        .build();
        dispatcherServiceFactory =
                createFactoryBasedOnStreamGraphs(
                        streamGraphs -> {
                            assertThat(streamGraphs).containsExactlyInAnyOrder(STREAM_GRAPH);

                            return TestingDispatcherGatewayService.newBuilder()
                                    .setDispatcherGateway(dispatcherGateway)
                                    .build();
                        });

        try (final SessionDispatcherLeaderProcess dispatcherLeaderProcess =
                createDispatcherLeaderProcess()) {
            dispatcherLeaderProcess.start();

            dispatcherLeaderProcess.getDispatcherGateway().get();

            dispatcherLeaderProcess.onAddedStreamGraph(STREAM_GRAPH.getJobId());

            verificationLogic.accept(fatalErrorHandler);
        }
    }

    private AbstractDispatcherLeaderProcess.DispatcherGatewayServiceFactory
            createFactoryBasedOnStreamGraphs(
                    Function<
                                    Collection<StreamGraph>,
                                    AbstractDispatcherLeaderProcess.DispatcherGatewayService>
                            createFunction) {
        return (ignoredDispatcherId,
                recoveredJobs,
                ignoredRecoveredDirtyJobResults,
                ignoredStreamGraphWriter,
                ignoredJobResultStore) -> createFunction.apply(recoveredJobs);
    }

    private AbstractDispatcherLeaderProcess.DispatcherGatewayServiceFactory
            createFactoryBasedOnGenericSupplier(
                    Supplier<AbstractDispatcherLeaderProcess.DispatcherGatewayService> supplier) {
        return (ignoredDispatcherId,
                ignoredRecoveredJobs,
                ignoredRecoveredDirtyJobResults,
                ignoredStreamGraphWriter,
                ignoredJobResultStore) -> supplier.get();
    }

    private void verifyOnAddedStreamGraphResultDidNotFail(
            TestingFatalErrorHandler fatalErrorHandler) {
        assertThatThrownBy(
                        () -> fatalErrorHandler.getErrorFuture().get(10L, TimeUnit.MILLISECONDS),
                        "Expected that duplicate job submissions due to false job recoveries are ignored.")
                .isInstanceOf(TimeoutException.class);
    }

    private SessionDispatcherLeaderProcess createDispatcherLeaderProcess() {
        return SessionDispatcherLeaderProcess.create(
                leaderSessionId,
                dispatcherServiceFactory,
                streamGraphStore,
                jobResultStore,
                ioExecutor,
                fatalErrorHandler);
    }
}
