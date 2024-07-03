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

package org.apache.flink.runtime.testutils;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobResourceRequirements;
import org.apache.flink.runtime.jobmanager.StreamGraphStore;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.BiConsumerWithException;
import org.apache.flink.util.function.BiFunctionWithException;
import org.apache.flink.util.function.FunctionWithException;
import org.apache.flink.util.function.ThrowingConsumer;
import org.apache.flink.util.function.ThrowingRunnable;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;

/** In-Memory implementation of {@link StreamGraphStore} for testing purposes. */
public class TestingStreamGraphStore implements StreamGraphStore {

    private final Map<JobID, StreamGraph> storedJobs = new HashMap<>();

    private final ThrowingConsumer<StreamGraphListener, ? extends Exception> startConsumer;

    private final ThrowingRunnable<? extends Exception> stopRunnable;

    private final FunctionWithException<Collection<JobID>, Collection<JobID>, ? extends Exception>
            jobIdsFunction;

    private final BiFunctionWithException<
                    JobID, Map<JobID, StreamGraph>, StreamGraph, ? extends Exception>
            recoverStreamGraphFunction;

    private final ThrowingConsumer<StreamGraph, ? extends Exception> putStreamGraphConsumer;

    private final BiConsumerWithException<StreamGraph, JobResourceRequirements, ? extends Exception>
            putJobResourceRequirementsConsumer;

    private final BiFunction<JobID, Executor, CompletableFuture<Void>> globalCleanupFunction;

    private final BiFunction<JobID, Executor, CompletableFuture<Void>> localCleanupFunction;

    private boolean started;

    private TestingStreamGraphStore(
            ThrowingConsumer<StreamGraphListener, ? extends Exception> startConsumer,
            ThrowingRunnable<? extends Exception> stopRunnable,
            FunctionWithException<Collection<JobID>, Collection<JobID>, ? extends Exception>
                    jobIdsFunction,
            BiFunctionWithException<
                            JobID, Map<JobID, StreamGraph>, StreamGraph, ? extends Exception>
                    recoverStreamGraphFunction,
            ThrowingConsumer<StreamGraph, ? extends Exception> putStreamGraphConsumer,
            BiConsumerWithException<StreamGraph, JobResourceRequirements, ? extends Exception>
                    putJobResourceRequirementsConsumer,
            BiFunction<JobID, Executor, CompletableFuture<Void>> globalCleanupFunction,
            BiFunction<JobID, Executor, CompletableFuture<Void>> localCleanupFunction,
            Collection<StreamGraph> initialStreamGraphs) {
        this.startConsumer = startConsumer;
        this.stopRunnable = stopRunnable;
        this.jobIdsFunction = jobIdsFunction;
        this.recoverStreamGraphFunction = recoverStreamGraphFunction;
        this.putStreamGraphConsumer = putStreamGraphConsumer;
        this.putJobResourceRequirementsConsumer = putJobResourceRequirementsConsumer;
        this.globalCleanupFunction = globalCleanupFunction;
        this.localCleanupFunction = localCleanupFunction;

        for (StreamGraph initialStreamGraph : initialStreamGraphs) {
            storedJobs.put(initialStreamGraph.getJobId(), initialStreamGraph);
        }
    }

    @Override
    public synchronized void start(@Nullable StreamGraphListener streamGraphListener)
            throws Exception {
        startConsumer.accept(streamGraphListener);
        started = true;
    }

    @Override
    public synchronized void stop() throws Exception {
        stopRunnable.run();
        started = false;
    }

    @Override
    public synchronized StreamGraph recoverStreamGraph(JobID jobId) throws Exception {
        verifyIsStarted();
        return recoverStreamGraphFunction.apply(jobId, storedJobs);
    }

    @Override
    public synchronized void putStreamGraph(StreamGraph streamGraph) throws Exception {
        verifyIsStarted();
        putStreamGraphConsumer.accept(streamGraph);
        storedJobs.put(streamGraph.getJobId(), streamGraph);
    }

    @Override
    public void putJobResourceRequirements(
            JobID jobId, JobResourceRequirements jobResourceRequirements) throws Exception {
        verifyIsStarted();
        final StreamGraph streamGraph =
                Preconditions.checkNotNull(storedJobs.get(jobId), "Job [%s] not found.", jobId);
        putJobResourceRequirementsConsumer.accept(streamGraph, jobResourceRequirements);
    }

    @Override
    public synchronized CompletableFuture<Void> globalCleanupAsync(JobID jobId, Executor executor) {
        verifyIsStarted();
        return globalCleanupFunction.apply(jobId, executor).thenRun(() -> storedJobs.remove(jobId));
    }

    @Override
    public synchronized CompletableFuture<Void> localCleanupAsync(JobID jobId, Executor executor) {
        verifyIsStarted();
        return localCleanupFunction.apply(jobId, executor);
    }

    @Override
    public synchronized Collection<JobID> getJobIds() throws Exception {
        verifyIsStarted();
        return jobIdsFunction.apply(
                Collections.unmodifiableSet(new HashSet<>(storedJobs.keySet())));
    }

    public synchronized boolean contains(JobID jobId) {
        return storedJobs.containsKey(jobId);
    }

    private void verifyIsStarted() {
        Preconditions.checkState(started, "Not running. Forgot to call start()?");
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /** {@code Builder} for creating {@code TestingStreamGraphStore} instances. */
    public static class Builder {
        private ThrowingConsumer<StreamGraphListener, ? extends Exception> startConsumer =
                ignored -> {};

        private ThrowingRunnable<? extends Exception> stopRunnable = () -> {};

        private FunctionWithException<Collection<JobID>, Collection<JobID>, ? extends Exception>
                jobIdsFunction = jobIds -> jobIds;

        private BiFunctionWithException<
                        JobID, Map<JobID, StreamGraph>, StreamGraph, ? extends Exception>
                recoverStreamGraphFunction = (jobId, jobs) -> jobs.get(jobId);

        private ThrowingConsumer<StreamGraph, ? extends Exception> putStreamGraphConsumer =
                ignored -> {};

        private BiConsumerWithException<StreamGraph, JobResourceRequirements, ? extends Exception>
                putJobResourceRequirementsConsumer = (graph, requirements) -> {};

        private BiFunction<JobID, Executor, CompletableFuture<Void>> globalCleanupFunction =
                (ignoredJobId, ignoredExecutor) -> FutureUtils.completedVoidFuture();

        private BiFunction<JobID, Executor, CompletableFuture<Void>> localCleanupFunction =
                (ignoredJobId, ignoredExecutor) -> FutureUtils.completedVoidFuture();

        private Collection<StreamGraph> initialStreamGraphs = Collections.emptyList();

        private boolean startStreamGraphStore = false;

        private Builder() {}

        public Builder setStartConsumer(
                ThrowingConsumer<StreamGraphListener, ? extends Exception> startConsumer) {
            this.startConsumer = startConsumer;
            return this;
        }

        public Builder setStopRunnable(ThrowingRunnable<? extends Exception> stopRunnable) {
            this.stopRunnable = stopRunnable;
            return this;
        }

        public Builder setJobIdsFunction(
                FunctionWithException<Collection<JobID>, Collection<JobID>, ? extends Exception>
                        jobIdsFunction) {
            this.jobIdsFunction = jobIdsFunction;
            return this;
        }

        public Builder setRecoverStreamGraphFunction(
                BiFunctionWithException<
                                JobID, Map<JobID, StreamGraph>, StreamGraph, ? extends Exception>
                        recoverStreamGraphFunction) {
            this.recoverStreamGraphFunction = recoverStreamGraphFunction;
            return this;
        }

        public Builder setPutStreamGraphConsumer(
                ThrowingConsumer<StreamGraph, ? extends Exception> putStreamGraphConsumer) {
            this.putStreamGraphConsumer = putStreamGraphConsumer;
            return this;
        }

        public Builder setPutJobResourceRequirementsConsumer(
                BiConsumerWithException<StreamGraph, JobResourceRequirements, ? extends Exception>
                        putJobResourceRequirementsConsumer) {
            this.putJobResourceRequirementsConsumer = putJobResourceRequirementsConsumer;
            return this;
        }

        public Builder setGlobalCleanupFunction(
                BiFunction<JobID, Executor, CompletableFuture<Void>> globalCleanupFunction) {
            this.globalCleanupFunction = globalCleanupFunction;
            return this;
        }

        public Builder setLocalCleanupFunction(
                BiFunction<JobID, Executor, CompletableFuture<Void>> localCleanupFunction) {
            this.localCleanupFunction = localCleanupFunction;
            return this;
        }

        public Builder setInitialStreamGraphs(Collection<StreamGraph> initialStreamGraphs) {
            this.initialStreamGraphs = initialStreamGraphs;
            return this;
        }

        public Builder withAutomaticStart() {
            this.startStreamGraphStore = true;
            return this;
        }

        public TestingStreamGraphStore build() {
            final TestingStreamGraphStore streamGraphStore =
                    new TestingStreamGraphStore(
                            startConsumer,
                            stopRunnable,
                            jobIdsFunction,
                            recoverStreamGraphFunction,
                            putStreamGraphConsumer,
                            putJobResourceRequirementsConsumer,
                            globalCleanupFunction,
                            localCleanupFunction,
                            initialStreamGraphs);

            if (startStreamGraphStore) {
                try {
                    streamGraphStore.start(null);
                } catch (Exception e) {
                    ExceptionUtils.rethrow(e);
                }
            }

            return streamGraphStore;
        }
    }
}
