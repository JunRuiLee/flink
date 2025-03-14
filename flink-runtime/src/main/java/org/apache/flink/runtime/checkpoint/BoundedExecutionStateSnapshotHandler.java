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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.FinishedTaskStateProvider.PartialFinishingNotSupportedByStateException;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinatorHolder;
import org.apache.flink.runtime.persistence.PossibleInconsistentStateException;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CheckpointStorageCoordinatorView;
import org.apache.flink.runtime.state.CheckpointStorageLocation;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

import static java.util.stream.Collectors.toMap;
import static org.apache.flink.runtime.checkpoint.Checkpoints.checkParallelismForAllJobVertices;
import static org.apache.flink.runtime.checkpoint.Checkpoints.getExecutionJobVertexOperatorStates;
import static org.apache.flink.runtime.checkpoint.Checkpoints.loadCheckpointMetadata;
import static org.apache.flink.util.ExceptionUtils.findThrowable;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The checkpoint coordinator coordinates the distributed snapshots of operators and state. It
 * triggers the checkpoint by sending the messages to the relevant tasks and collects the checkpoint
 * acknowledgements. It also collects and maintains the overview of the state handles reported by
 * the tasks that acknowledge the checkpoint.
 *
 * <p>TODO: Not support master hook
 */
public class BoundedExecutionStateSnapshotHandler {

    private static final Logger LOG =
            LoggerFactory.getLogger(BoundedExecutionStateSnapshotHandler.class);

    /** The number of recent checkpoints whose IDs are remembered. */
    private static final int NUM_GHOST_CHECKPOINT_IDS = 16;

    // ------------------------------------------------------------------------

    /** Coordinator-wide lock to safeguard the checkpoint updates. */
    private final Object lock = new Object();

    /** The job whose checkpoint this coordinator coordinates. */
    private final JobID job;

    /** Default checkpoint properties. */
    private final CheckpointProperties checkpointProperties;

    /** The executor used for asynchronous calls, like potentially blocking I/O. */
    private final Executor executor;

    private final CheckpointsCleaner checkpointsCleaner;
    /**
     * The root checkpoint state backend, which is responsible for initializing the checkpoint,
     * storing the metadata, and cleaning up the checkpoint.
     */
    private final CheckpointStorageCoordinatorView checkpointStorageView;

    /** A list of recent expired checkpoint IDs, to identify late messages (vs invalid ones). */
    private final ArrayDeque<Long> recentExpiredCheckpoints;

    /**
     * Checkpoint ID counter to ensure ascending IDs. In case of job manager failures, these need to
     * be ascending across job managers.
     */
    private final CheckpointIDCounter checkpointIdCounter;

    /** The max time (in ms) that a checkpoint may take. */
    private final long checkpointTimeout;

    private final CompletedCheckpointStore completedCheckpointStore;

    /**
     * The timer that handles the checkpoint timeouts and triggers periodic checkpoints. It must be
     * single-threaded. Eventually it will be replaced by main thread executor.
     */
    private final ScheduledExecutor timer;

    /** Optional tracker for checkpoint statistics. */
    private final CheckpointStatsTracker statsTracker;

    private final CheckpointFailureManager failureManager;

    private final CompletableFuture<Void> completeFuture = new CompletableFuture<>();
    private final CompletableFuture<Void> restoreFuture = new CompletableFuture<>();

    private BoundedExecutionPendingCheckpoint pendingCheckpoint;
    private final CompletableFuture<CheckpointStorageLocation> checkpointStorageLocationFuture;
    private final Supplier<Integer> pendingOperatorsCountRetriever;
    private CheckpointOptions checkpointOptions;
    private CheckpointMetaData checkpointMeta;
    private @Nullable CompletedCheckpoint restoredCheckpoint;
    private @Nullable CheckpointMetadata restoredCheckpointMetaData;
    private boolean allowNonRestored;

    // --------------------------------------------------------------------------------------------

    public BoundedExecutionStateSnapshotHandler(
            JobID job,
            CheckpointCoordinatorConfiguration chkConfig,
            CheckpointIDCounter checkpointIDCounter,
            CompletedCheckpointStore completedCheckpointStore,
            CheckpointStorage checkpointStorage,
            Executor executor,
            CheckpointsCleaner checkpointsCleaner,
            ScheduledExecutor timer,
            CheckpointFailureManager failureManager,
            CheckpointStatsTracker statsTracker,
            Supplier<Integer> pendingOperatorsCountRetriever) {

        // sanity checks
        checkNotNull(checkpointStorage);

        this.job = checkNotNull(job);
        this.checkpointTimeout = chkConfig.getCheckpointTimeout();
        this.checkpointIdCounter = checkNotNull(checkpointIDCounter);
        this.completedCheckpointStore = checkNotNull(completedCheckpointStore);
        this.executor = checkNotNull(executor);
        this.checkpointsCleaner = checkNotNull(checkpointsCleaner);
        this.failureManager = checkNotNull(failureManager);
        this.recentExpiredCheckpoints = new ArrayDeque<>(NUM_GHOST_CHECKPOINT_IDS);

        this.timer = timer;

        this.checkpointProperties =
                new CheckpointProperties(
                        false,
                        CheckpointType.CHECKPOINT,
                        true,
                        false, // Retain on success
                        false, // Retain on cancellation
                        false, // Retain on failure
                        false, // Retain on suspension
                        false);

        this.pendingOperatorsCountRetriever = checkNotNull(pendingOperatorsCountRetriever);

        try {
            this.checkpointStorageView = checkpointStorage.createCheckpointStorage(job);

            checkpointStorageView.initializeBaseLocationsForCheckpoint();
        } catch (IOException e) {
            throw new FlinkRuntimeException(
                    "Failed to create checkpoint storage at checkpoint coordinator side.", e);
        }

        try {
            // Make sure the checkpoint ID enumerator is running. Possibly
            // issues a blocking call to ZooKeeper.
            checkpointIDCounter.start();
        } catch (Throwable t) {
            throw new RuntimeException(
                    "Failed to start checkpoint ID counter: " + t.getMessage(), t);
        }
        this.statsTracker = checkNotNull(statsTracker, "Statistic tracker can not be null");

        // TODO initial global restored state, e.g checkpoint id

        // this call may consume much time, so we have to put it to executor
        this.checkpointStorageLocationFuture =
                restoreFuture.thenApplyAsync(
                        ignored -> {
                            try {
                                checkpointStorageView.initializeBaseLocationsForCheckpoint();
                                return checkpointStorageView.initializeLocationForCheckpoint(
                                        checkpointIDCounter.get());
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        },
                        executor);
    }

    public CheckpointMetaData getCheckpointMeta() {
        return checkpointMeta;
    }

    public void initialize() {
        // TODO initial some global context for restore
    }

    private void restoreJobVertices(ExecutionJobVertex executionJobVertex) throws Exception {
        checkState(executionJobVertex.isInitialized(), "job vertex is not initialized");
        reAssignStatesIfExecutionJobVertexInitialized(executionJobVertex);
    }

    // since savepoint do not have incremental mode, so here we will use checkpoint
    public void startWaitCheckpointForFinishedJob() {
        // 1. at first create a pending checkpoint with empty graph, if an exception occurs, we
        // should trigger global failure or jm fatal error (TODO)
        this.pendingCheckpoint = createPendingCheckpoint();

        // 2. notify pending checkpoint the new job vertices initialized, and the pending checkpoint
        // should wait acknowledge tasks snapshot.
        // 3. notify job vertices finished and trigger to snapshot the coordinator of these job
        // vertices async.

        // 4. any exception of step 2 and 3, we should throw an exception to trigger related tasks
        // restarts, and clear their states
        // TODO
    }

    public CheckpointOptions getCheckpointOptions() {
        return checkpointOptions;
    }

    private BoundedExecutionPendingCheckpoint createPendingCheckpoint() {
        try {
            synchronized (lock) {
                preCheckGlobalState();
            }

            CheckpointTriggerRequest request =
                    new CheckpointTriggerRequest(checkpointProperties, null, false);

            final long timestamp = System.currentTimeMillis();

            long checkpointID = checkpointIdCounter.get();

            BoundedExecutionPendingCheckpoint pendingCheckpoint =
                    createPendingCheckpoint(
                            timestamp,
                            request.props,
                            checkpointID,
                            request.getOnCompletionFuture(),
                            pendingOperatorsCountRetriever);

            CheckpointStorageLocation checkpointStorageLocation =
                    checkpointStorageLocationFuture.join();
            pendingCheckpoint.setCheckpointTargetLocation(checkpointStorageLocation);

            this.checkpointOptions =
                    CheckpointOptions.alignedNoTimeout(
                            checkpointProperties.getCheckpointType(),
                            checkpointStorageLocation.getLocationReference());

            this.checkpointMeta = new CheckpointMetaData(checkpointID, timestamp);

            return pendingCheckpoint;
        } catch (Throwable throwable) {
            throw new RuntimeException(
                    "Failed to create bounded execution pending checkpoint", throwable);
        }
    }

    private BoundedExecutionPendingCheckpoint createPendingCheckpoint(
            long timestamp,
            CheckpointProperties props,
            long checkpointID,
            CompletableFuture<CompletedCheckpoint> onCompletionPromise,
            Supplier<Integer> pendingOperatorsCountRetriever) {

        // TODO currently we could not reuse the stats tracker because the job plan is not decided
        // when enable dynamic graph
        // PendingCheckpointStats pendingCheckpointStats =
        // trackPendingCheckpointStats(checkpointID, props, timestamp);

        final BoundedExecutionPendingCheckpoint checkpoint =
                new BoundedExecutionPendingCheckpoint(
                        job,
                        checkpointID,
                        props,
                        onCompletionPromise,
                        null,
                        pendingOperatorsCountRetriever);

        LOG.info(
                "Triggering checkpoint {} (type={}) @ {} for job {}.",
                checkpointID,
                checkpoint.getProps().getCheckpointType(),
                timestamp,
                job);
        return checkpoint;
    }

    /**
     * The trigger request is failed prematurely without a proper initialization. There is no
     * resource to release, but the completion promise needs to fail manually here.
     *
     * @param onCompletionPromise the completion promise of the checkpoint/savepoint
     * @param throwable the reason of trigger failure
     */
    private void onTriggerFailure(
            CheckpointTriggerRequest onCompletionPromise, Throwable throwable) {
        final CheckpointException checkpointException =
                getCheckpointException(
                        CheckpointFailureReason.TRIGGER_CHECKPOINT_FAILURE, throwable);
        onCompletionPromise.completeExceptionally(checkpointException);
        onTriggerFailure(onCompletionPromise.props, checkpointException);
    }

    private void triggerGlobalJobFailure(Throwable throwable) {
        // TODO
    }

    /**
     * The trigger request is failed. NOTE, it must be invoked if trigger request is failed.
     *
     * <p>the pending checkpoint which is failed. It could be null if it's failed prematurely
     * without a proper initialization.
     *
     * @param throwable the reason of trigger failure
     */
    private void onTriggerFailure(CheckpointProperties checkpointProperties, Throwable throwable) {
        try {
            // beautify the stack trace a bit
            throwable = ExceptionUtils.stripCompletionException(throwable);

            final CheckpointException cause =
                    getCheckpointException(
                            CheckpointFailureReason.TRIGGER_CHECKPOINT_FAILURE, throwable);

            failureManager.handleCheckpointException(
                    pendingCheckpoint, checkpointProperties, cause, null, job, null, statsTracker);
        } catch (Throwable secondThrowable) {
            secondThrowable.addSuppressed(throwable);
            throw secondThrowable;
        }
    }

    public void onJobVertexInitialized(ExecutionJobVertex executionJobVertex) throws Exception {
        restoreJobVertices(executionJobVertex);
        pendingCheckpoint.onJobVertexInitialized(executionJobVertex);
    }

    public void onOperatorCoordinatorInitialized(
            OperatorCoordinatorHolder operatorCoordinatorHolder) {
        operatorCoordinatorHolder.markForCheckpoint(checkpointIdCounter.get());
    }

    public void onExecutionFailure(List<Execution> executionsToFailed) {
        // TODO mark checkpoint as disposed to drop all later ack message, and wait to be notified
        // which tasks will be restarted
    }

    public void onJobVertexFinished(ExecutionJobVertex executionJobVertex) {
        pendingCheckpoint.onJobVertexFinished(executionJobVertex);

        // trigger snapshot of operator coordinators
        Collection<OperatorCoordinatorHolder> operatorCoordinators =
                executionJobVertex.getOperatorCoordinators();
        Collection<OperatorCoordinatorCheckpointContext> operatorsToCheckpoint = new ArrayList<>();
        operatorsToCheckpoint.addAll(operatorCoordinators);
        this.triggeredCheckpointOperatoraCoordinators.addAll(operatorsToCheckpoint);

        CompletableFuture.runAsync(
                () ->
                        OperatorCoordinatorCheckpoints
                                .triggerAndAcknowledgeAllCoordinatorCheckpointsWithCompletion(
                                        operatorsToCheckpoint, pendingCheckpoint, timer),
                timer);
    }

    // --------------------------------------------------------------------------------------------
    //  Handling checkpoints and messages
    // --------------------------------------------------------------------------------------------

    /**
     * Receives an AcknowledgeCheckpoint message and returns whether the message was associated with
     * a pending checkpoint.
     *
     * @param message Checkpoint ack from the task manager
     * @param taskManagerLocationInfo The location of the acknowledge checkpoint message's sender
     * @return Flag indicating whether the ack'd checkpoint was associated with a pending
     *     checkpoint.
     * @throws CheckpointException If the checkpoint cannot be added to the completed checkpoint
     *     store.
     */
    public boolean receiveAcknowledgeMessage(
            AcknowledgeCheckpoint message, String taskManagerLocationInfo)
            throws CheckpointException {
        if (message == null) {
            return false;
        }

        if (!job.equals(message.getJob())) {
            LOG.error(
                    "Received wrong AcknowledgeCheckpoint message for job {} from {} : {}",
                    job,
                    taskManagerLocationInfo,
                    message);
            return false;
        }

        final long checkpointId = message.getCheckpointId();

        synchronized (lock) {
            final BoundedExecutionPendingCheckpoint checkpoint = pendingCheckpoint;
            checkState(checkpointId == getCheckpointId(), "Received message for wrong checkpoint");

            if (message.getSubtaskState() != null) {
                // Register shared state regardless of checkpoint state and task ACK state.
                // This way, shared state is
                // 1. kept if the message is late or state will be used by the task otherwise
                // 2. removed eventually upon checkpoint subsumption (or job cancellation)
                // Do not register savepoints' shared state, as Flink is not in charge of
                // savepoints' lifecycle
                if (checkpoint == null || !checkpoint.getProps().isSavepoint()) {
                    message.getSubtaskState()
                            .registerSharedStates(
                                    completedCheckpointStore.getSharedStateRegistry(),
                                    checkpointId);
                }
            }

            if (checkpoint != null && !checkpoint.isDisposed()) {

                switch (checkpoint.acknowledgeTask(
                        message.getTaskExecutionId(),
                        message.getSubtaskState(),
                        message.getCheckpointMetrics())) {
                    case SUCCESS:
                        LOG.info(
                                "Received acknowledge message for checkpoint {} from task {} of job {} at {}.",
                                checkpointId,
                                message.getTaskExecutionId(),
                                message.getJob(),
                                taskManagerLocationInfo);

                        if (checkpoint.isFullyAcknowledged()) {
                            completePendingCheckpoint(checkpoint);
                        }
                        break;
                    case DUPLICATE:
                        LOG.info(
                                "Received a duplicate acknowledge message for checkpoint {}, task {}, job {}, location {}.",
                                message.getCheckpointId(),
                                message.getTaskExecutionId(),
                                message.getJob(),
                                taskManagerLocationInfo);
                        break;
                    case UNKNOWN:
                        LOG.warn(
                                "Could not acknowledge the checkpoint {} for task {} of job {} at {}, "
                                        + "because the task's execution attempt id was unknown. Discarding "
                                        + "the state handle to avoid lingering state.",
                                message.getCheckpointId(),
                                message.getTaskExecutionId(),
                                message.getJob(),
                                taskManagerLocationInfo);

                        discardSubtaskState(
                                message.getJob(),
                                message.getTaskExecutionId(),
                                message.getCheckpointId(),
                                message.getSubtaskState());

                        break;
                    case DISCARDED:
                        LOG.warn(
                                "Could not acknowledge the checkpoint {} for task {} of job {} at {}, "
                                        + "because the pending checkpoint had been discarded. Discarding the "
                                        + "state handle tp avoid lingering state.",
                                message.getCheckpointId(),
                                message.getTaskExecutionId(),
                                message.getJob(),
                                taskManagerLocationInfo);

                        discardSubtaskState(
                                message.getJob(),
                                message.getTaskExecutionId(),
                                message.getCheckpointId(),
                                message.getSubtaskState());
                }

                return true;
            } else if (checkpoint != null) {
                // this should not happen
                throw new IllegalStateException(
                        "Received message for discarded but non-removed checkpoint "
                                + checkpointId);
            } else {
                reportCheckpointMetrics(
                        message.getCheckpointId(),
                        message.getTaskExecutionId(),
                        message.getCheckpointMetrics());
                boolean wasPendingCheckpoint;

                // message is for an unknown checkpoint, or comes too late (checkpoint disposed)
                if (recentExpiredCheckpoints.contains(checkpointId)) {
                    wasPendingCheckpoint = true;
                    LOG.warn(
                            "Received late message for now expired checkpoint attempt {} from task "
                                    + "{} of job {} at {}.",
                            checkpointId,
                            message.getTaskExecutionId(),
                            message.getJob(),
                            taskManagerLocationInfo);
                } else {
                    LOG.debug(
                            "Received message for an unknown checkpoint {} from task {} of job {} at {}.",
                            checkpointId,
                            message.getTaskExecutionId(),
                            message.getJob(),
                            taskManagerLocationInfo);
                    wasPendingCheckpoint = false;
                }

                // try to discard the state so that we don't have lingering state lying around
                discardSubtaskState(
                        message.getJob(),
                        message.getTaskExecutionId(),
                        message.getCheckpointId(),
                        message.getSubtaskState());

                return wasPendingCheckpoint;
            }
        }
    }

    /**
     * Try to complete the given pending checkpoint.
     *
     * <p>Important: This method should only be called in the checkpoint lock scope.
     *
     * @param pendingCheckpoint to complete
     * @throws CheckpointException if the completion failed
     */
    private void completePendingCheckpoint(BoundedExecutionPendingCheckpoint pendingCheckpoint)
            throws CheckpointException {
        final long checkpointId = pendingCheckpoint.getCheckpointID();
        final CompletedCheckpoint completedCheckpoint;
        final CompletedCheckpoint lastSubsumed;
        final CheckpointProperties props = pendingCheckpoint.getProps();

        completedCheckpointStore.getSharedStateRegistry().checkpointCompleted(checkpointId);

        try {
            completedCheckpoint = finalizeCheckpoint(pendingCheckpoint);

            // the pending checkpoint must be discarded after the finalization
            Preconditions.checkState(pendingCheckpoint.isDisposed() && completedCheckpoint != null);

            if (!props.isSavepoint()) {
                lastSubsumed =
                        addCompletedCheckpointToStoreAndSubsumeOldest(
                                checkpointId, completedCheckpoint, pendingCheckpoint);
            } else {
                lastSubsumed = null;
            }

            pendingCheckpoint.getCompletionFuture().complete(completedCheckpoint);
            reportCompletedCheckpoint(completedCheckpoint);
        } catch (Exception exception) {
            // For robustness reasons, we need catch exception and try marking the checkpoint
            // completed.
            pendingCheckpoint.getCompletionFuture().completeExceptionally(exception);
            throw exception;
        } finally {
            markCheckpointCompleted();
        }

        cleanupAfterCompletedCheckpoint(
                pendingCheckpoint, checkpointId, completedCheckpoint, lastSubsumed, props);
    }

    private void markCheckpointCompleted() {
        this.completeFuture.complete(null);
    }

    private void reportCompletedCheckpoint(CompletedCheckpoint completedCheckpoint) {
        failureManager.handleCheckpointSuccess(completedCheckpoint.getCheckpointID());
        CompletedCheckpointStats completedCheckpointStats = completedCheckpoint.getStatistic();
        if (completedCheckpointStats != null) {
            LOG.trace(
                    "Checkpoint {} size: {}Kb, duration: {}ms",
                    completedCheckpoint.getCheckpointID(),
                    completedCheckpointStats.getStateSize() == 0
                            ? 0
                            : completedCheckpointStats.getStateSize() / 1024,
                    completedCheckpointStats.getEndToEndDuration());
            // Finalize the statsCallback and give the completed checkpoint a
            // callback for discards.
            statsTracker.reportCompletedCheckpoint(completedCheckpointStats);
        }
    }

    private void cleanupAfterCompletedCheckpoint(
            BoundedExecutionPendingCheckpoint pendingCheckpoint,
            long checkpointId,
            CompletedCheckpoint completedCheckpoint,
            CompletedCheckpoint lastSubsumed,
            CheckpointProperties props) {

        logCheckpointInfo(completedCheckpoint);

        if (!props.isSavepoint() || props.isSynchronous()) {
            // send the "notify complete" call to all vertices, coordinators, etc.
            sendAcknowledgeMessages(
                    pendingCheckpoint.getTasksToCommitTo(),
                    checkpointId,
                    completedCheckpoint.getTimestamp(),
                    extractIdIfDiscardedOnSubsumed(lastSubsumed));
        }
    }

    private void logCheckpointInfo(CompletedCheckpoint completedCheckpoint) {
        LOG.info(
                "Completed checkpoint {} for job {} ({} bytes, checkpointDuration={} ms, finalizationTime={} ms).",
                completedCheckpoint.getCheckpointID(),
                job,
                completedCheckpoint.getStateSize(),
                completedCheckpoint.getCompletionTimestamp() - completedCheckpoint.getTimestamp(),
                System.currentTimeMillis() - completedCheckpoint.getCompletionTimestamp());

        if (LOG.isDebugEnabled()) {
            StringBuilder builder = new StringBuilder();
            builder.append("Checkpoint state: ");
            for (OperatorState state : completedCheckpoint.getOperatorStates().values()) {
                builder.append(state);
                builder.append(", ");
            }
            // Remove last two chars ", "
            builder.setLength(builder.length() - 2);

            LOG.debug(builder.toString());
        }
    }

    private CompletedCheckpoint finalizeCheckpoint(
            BoundedExecutionPendingCheckpoint pendingCheckpoint) throws CheckpointException {
        try {
            final CompletedCheckpoint completedCheckpoint =
                    pendingCheckpoint.finalizeCheckpoint(
                            checkpointsCleaner, this::postCleanup, executor);

            return completedCheckpoint;
        } catch (Exception e1) {
            // abort the current pending checkpoint if we fails to finalize the pending
            // checkpoint.
            final CheckpointFailureReason failureReason =
                    e1 instanceof PartialFinishingNotSupportedByStateException
                            ? CheckpointFailureReason.CHECKPOINT_DECLINED_TASK_CLOSING
                            : CheckpointFailureReason.FINALIZE_CHECKPOINT_FAILURE;

            if (!pendingCheckpoint.isDisposed()) {
                abortPendingCheckpoint(
                        pendingCheckpoint, new CheckpointException(failureReason, e1));
            }

            throw new CheckpointException(
                    "Could not finalize the pending checkpoint "
                            + pendingCheckpoint.getCheckpointID()
                            + '.',
                    failureReason,
                    e1);
        }
    }

    private void postCleanup() {
        // TODO
    }

    private long extractIdIfDiscardedOnSubsumed(CompletedCheckpoint lastSubsumed) {
        final long lastSubsumedCheckpointId;
        if (lastSubsumed != null && lastSubsumed.getProperties().discardOnSubsumed()) {
            lastSubsumedCheckpointId = lastSubsumed.getCheckpointID();
        } else {
            lastSubsumedCheckpointId = CheckpointStoreUtil.INVALID_CHECKPOINT_ID;
        }
        return lastSubsumedCheckpointId;
    }

    private CompletedCheckpoint addCompletedCheckpointToStoreAndSubsumeOldest(
            long checkpointId,
            CompletedCheckpoint completedCheckpoint,
            BoundedExecutionPendingCheckpoint pendingCheckpoint)
            throws CheckpointException {
        List<ExecutionVertex> tasksToAbort = pendingCheckpoint.getTasksToCommitTo();
        try {
            final CompletedCheckpoint subsumedCheckpoint =
                    completedCheckpointStore.addCheckpointAndSubsumeOldestOne(
                            completedCheckpoint, checkpointsCleaner, this::postCleanup);
            return subsumedCheckpoint;
        } catch (Exception exception) {
            pendingCheckpoint.getCompletionFuture().completeExceptionally(exception);
            if (exception instanceof PossibleInconsistentStateException) {
                LOG.warn(
                        "An error occurred while writing checkpoint {} to the underlying metadata"
                                + " store. Flink was not able to determine whether the metadata was"
                                + " successfully persisted. The corresponding state located at '{}'"
                                + " won't be discarded and needs to be cleaned up manually.",
                        completedCheckpoint.getCheckpointID(),
                        completedCheckpoint.getExternalPointer());
            } else {
                // we failed to store the completed checkpoint. Let's clean up
                checkpointsCleaner.cleanCheckpointOnFailedStoring(completedCheckpoint, executor);
            }

            final CheckpointException checkpointException =
                    new CheckpointException(
                            "Could not complete the pending checkpoint " + checkpointId + '.',
                            CheckpointFailureReason.FINALIZE_CHECKPOINT_FAILURE,
                            exception);
            reportFailedCheckpoint(pendingCheckpoint, checkpointException);
            sendAbortedMessages(tasksToAbort, checkpointId, completedCheckpoint.getTimestamp());
            throw checkpointException;
        }
    }

    private void reportFailedCheckpoint(
            BoundedExecutionPendingCheckpoint pendingCheckpoint, CheckpointException exception) {

        failureManager.handleCheckpointException(
                pendingCheckpoint,
                pendingCheckpoint.getProps(),
                exception,
                null,
                job,
                getStatsCallback(pendingCheckpoint),
                statsTracker);
    }

    private final List<OperatorCoordinatorCheckpointContext>
            triggeredCheckpointOperatoraCoordinators = new ArrayList<>();

    @VisibleForTesting
    void sendAcknowledgeMessages(
            List<ExecutionVertex> tasksToCommit,
            long completedCheckpointId,
            long completedTimestamp,
            long lastSubsumedCheckpointId) {
        // commit tasks
        for (ExecutionVertex ev : tasksToCommit) {
            Execution ee = ev.getCurrentExecutionAttempt();
            if (ee != null) {
                ee.notifyCheckpointOnComplete(
                        completedCheckpointId, completedTimestamp, lastSubsumedCheckpointId);
            }
        }

        // commit coordinators
        for (OperatorCoordinatorCheckpointContext coordinatorContext :
                triggeredCheckpointOperatoraCoordinators) {
            coordinatorContext.notifyCheckpointComplete(completedCheckpointId);
        }
    }

    private void sendAbortedMessages(
            List<ExecutionVertex> tasksToAbort, long checkpointId, long timeStamp) {
        assert (Thread.holdsLock(lock));
        long latestCompletedCheckpointId = completedCheckpointStore.getLatestCheckpointId();

        // send notification of aborted checkpoints asynchronously.
        executor.execute(
                () -> {
                    // send the "abort checkpoint" messages to necessary vertices.
                    for (ExecutionVertex ev : tasksToAbort) {
                        Execution ee = ev.getCurrentExecutionAttempt();
                        if (ee != null) {
                            try {
                                ee.notifyCheckpointAborted(
                                        checkpointId, latestCompletedCheckpointId, timeStamp);
                            } catch (Throwable e) {
                                LOG.warn(
                                        "Could not send aborted message of checkpoint {} to task {} belonging to job {}.",
                                        checkpointId,
                                        ee.getAttemptId(),
                                        ee.getVertex().getJobId(),
                                        e);
                            }
                        }
                    }
                });

        // commit coordinators
        for (OperatorCoordinatorCheckpointContext coordinatorContext :
                triggeredCheckpointOperatoraCoordinators) {
            coordinatorContext.notifyCheckpointAborted(checkpointId);
        }
    }

    private void rememberRecentExpiredCheckpointId(long id) {
        if (recentExpiredCheckpoints.size() >= NUM_GHOST_CHECKPOINT_IDS) {
            recentExpiredCheckpoints.removeFirst();
        }
        recentExpiredCheckpoints.addLast(id);
    }

    public long getCheckpointId() {
        return pendingCheckpoint.getCheckpointID();
    }

    // --------------------------------------------------------------------------------------------
    //  Checkpoint State Restoring
    // --------------------------------------------------------------------------------------------

    public void reAssignStatesIfExecutionJobVertexInitialized(ExecutionJobVertex executionJobVertex)
            throws Exception {
        if (restoredCheckpoint == null) {
            return;
        }
        Set<ExecutionJobVertex> jobVertices = Set.of(executionJobVertex);
        // at first, check parallelism
        Checkpoints.validateParallelismByJobVertex(
                restoredCheckpoint.getOperatorStates(), jobVertices, restoredCheckpointMetaData);

        // 2.
        final Map<OperatorID, OperatorState> operatorStates =
                restoredCheckpoint.getOperatorStates();

        StateAssignmentOperation stateAssignmentOperation =
                new StateAssignmentOperation(
                        restoredCheckpoint.getCheckpointID(),
                        jobVertices,
                        getExecutionJobVertexOperatorStates(jobVertices, operatorStates),
                        // Since the assigned states are filter for the given job vertex, we don't
                        // need to
                        // allow non restored state
                        false);

        stateAssignmentOperation.assignStates();

        restoreStateToCoordinators(
                restoredCheckpoint.getCheckpointID(),
                operatorStates,
                executionJobVertex.getOperatorCoordinators());

        if (pendingOperatorsCountRetriever.get() == 0) {
            checkParallelismForAllJobVertices(
                    restoredCheckpointMetaData,
                    pendingCheckpoint.getExecutionJobVertices(),
                    restoredCheckpoint.getExternalPointer(),
                    allowNonRestored,
                    restoredCheckpoint.getOperatorStates());
        }
    }

    /**
     * Restore the state with given savepoint.
     *
     * @param restoreSettings Settings for a snapshot to restore from. Includes the path and
     *     parameters for the restore process.
     * @param userClassLoader The class loader to resolve serialized classes in legacy savepoint
     *     versions.
     */
    public void restoreSavepoint(
            SavepointRestoreSettings restoreSettings, ClassLoader userClassLoader)
            throws Exception {

        if (!restoreSettings.restoreSavepoint()) {
            restoreFuture.complete(null);
            return;
        }

        final String savepointPointer = restoreSettings.getRestorePath();
        this.allowNonRestored = restoreSettings.allowNonRestoredState();
        Preconditions.checkNotNull(savepointPointer, "The savepoint path cannot be null.");

        LOG.info(
                "Starting job {} from savepoint {} ({})",
                job,
                savepointPointer,
                (allowNonRestored ? "allowing non restored state" : ""));

        final CompletedCheckpointStorageLocation checkpointLocation =
                checkpointStorageView.resolveCheckpoint(savepointPointer);

        // convert to checkpoint so the system can fall back to it
        final CheckpointProperties checkpointProperties;
        switch (restoreSettings.getRecoveryClaimMode()) {
            case CLAIM:
                checkpointProperties = this.checkpointProperties;
                break;
            default:
                throw new IllegalArgumentException(
                        "Only supported claim mode because Flink should take ownership of this checkpoint.");
        }

        // Load the savepoint as a checkpoint into the system
        final StreamStateHandle metadataHandle = checkpointLocation.getMetadataHandle();
        final String checkpointPointer = checkpointLocation.getExternalPointer();

        // (1) load the savepoint
        final CheckpointMetadata checkpointMetadata;
        try (InputStream in = metadataHandle.openInputStream()) {
            DataInputStream dis = new DataInputStream(in);
            checkpointMetadata = loadCheckpointMetadata(dis, userClassLoader, checkpointPointer);
        }

        CompletedCheckpoint savepoint =
                Checkpoints.loadAndValidateCheckpoint(
                        job,
                        Collections.emptyMap(),
                        checkpointLocation,
                        userClassLoader,
                        checkpointProperties,
                        checkpointMetadata);

        // register shared state - even before adding the checkpoint to the store
        // because the latter might trigger subsumption so the ref counts must be up-to-date
        savepoint.registerSharedStatesAfterRestored(
                completedCheckpointStore.getSharedStateRegistry(),
                restoreSettings.getRecoveryClaimMode());

        completedCheckpointStore.addCheckpointAndSubsumeOldestOne(
                savepoint, checkpointsCleaner, () -> {});

        // Reset the checkpoint ID counter
        long nextCheckpointId = savepoint.getCheckpointID() + 1;
        checkpointIdCounter.setCount(nextCheckpointId);
        this.restoredCheckpoint = savepoint;
        this.restoredCheckpointMetaData = checkpointMetadata;

        LOG.info("Reset the checkpoint ID of job {} to {}.", job, nextCheckpointId);

        LOG.info("Restoring job {} from {}.", job, savepoint);

        restoreFuture.complete(null);
    }

    // ------------------------------------------------------------------------
    //  Accessors
    // ------------------------------------------------------------------------

    public CheckpointStorageCoordinatorView getCheckpointStorage() {
        return checkpointStorageView;
    }

    // --------------------------------------------------------------------------------------------
    //  Periodic scheduling of checkpoints
    // --------------------------------------------------------------------------------------------

    /**
     * Aborts all the pending checkpoints due to en exception.
     *
     * @param exception The exception.
     */
    public void abortPendingCheckpoints(CheckpointException exception) {
        synchronized (lock) {
            abortPendingCheckpoint(pendingCheckpoint, exception);
        }
    }

    private void restoreStateToCoordinators(
            final long checkpointId,
            final Map<OperatorID, OperatorState> operatorStates,
            Collection<? extends OperatorCoordinatorCheckpointContext>
                    toRestoredOperatorCoordinators)
            throws Exception {

        for (OperatorCoordinatorCheckpointContext coordContext : toRestoredOperatorCoordinators) {
            final OperatorState state = operatorStates.get(coordContext.operatorId());
            final ByteStreamStateHandle coordinatorState =
                    state == null ? null : state.getCoordinatorState();
            final byte[] bytes = coordinatorState == null ? null : coordinatorState.getData();
            coordContext.resetToCheckpoint(checkpointId, bytes);
        }
    }

    // ------------------------------------------------------------------------
    //  job status listener that schedules / cancels periodic checkpoints
    // ------------------------------------------------------------------------

    public void reportCheckpointMetrics(
            long id, ExecutionAttemptID attemptId, CheckpointMetrics metrics) {
        statsTracker.reportIncompleteStats(id, attemptId, metrics);
    }

    public void reportInitializationMetrics(
            ExecutionAttemptID executionAttemptID,
            SubTaskInitializationMetrics initializationMetrics) {
        statsTracker.reportInitializationMetrics(executionAttemptID, initializationMetrics);
    }

    /**
     * Discards the given state object asynchronously belonging to the given job, execution attempt
     * id and checkpoint id.
     *
     * @param jobId identifying the job to which the state object belongs
     * @param executionAttemptID identifying the task to which the state object belongs
     * @param checkpointId of the state object
     * @param subtaskState to discard asynchronously
     */
    private void discardSubtaskState(
            final JobID jobId,
            final ExecutionAttemptID executionAttemptID,
            final long checkpointId,
            final TaskStateSnapshot subtaskState) {

        if (subtaskState != null) {
            executor.execute(
                    new Runnable() {
                        @Override
                        public void run() {

                            try {
                                subtaskState.discardState();
                            } catch (Throwable t2) {
                                LOG.warn(
                                        "Could not properly discard state object of checkpoint {} "
                                                + "belonging to task {} of job {}.",
                                        checkpointId,
                                        executionAttemptID,
                                        jobId,
                                        t2);
                            }
                        }
                    });
        }
    }

    private void abortPendingCheckpoint(
            BoundedExecutionPendingCheckpoint pendingCheckpoint, CheckpointException exception) {

        abortPendingCheckpoint(pendingCheckpoint, exception, null);
    }

    private void abortPendingCheckpoint(
            BoundedExecutionPendingCheckpoint pendingCheckpoint,
            CheckpointException exception,
            @Nullable final ExecutionAttemptID executionAttemptID) {

        assert (Thread.holdsLock(lock));

        if (!pendingCheckpoint.isDisposed()) {
            try {
                // release resource here
                pendingCheckpoint.abort(
                        exception.getCheckpointFailureReason(),
                        exception.getCause(),
                        checkpointsCleaner,
                        this::postCleanup,
                        executor);

                failureManager.handleCheckpointException(
                        pendingCheckpoint,
                        pendingCheckpoint.getProps(),
                        exception,
                        executionAttemptID,
                        job,
                        getStatsCallback(pendingCheckpoint),
                        statsTracker);
            } finally {
                sendAbortedMessages(
                        pendingCheckpoint.getTasksToCommitTo(),
                        pendingCheckpoint.getCheckpointID(),
                        System.currentTimeMillis());
                if (exception
                        .getCheckpointFailureReason()
                        .equals(CheckpointFailureReason.CHECKPOINT_EXPIRED)) {
                    rememberRecentExpiredCheckpointId(pendingCheckpoint.getCheckpointID());
                }
            }
        }
    }

    private void preCheckGlobalState() throws CheckpointException {
        // TODO add some pre check
    }

    private static CheckpointException getCheckpointException(
            CheckpointFailureReason defaultReason, Throwable throwable) {

        final Optional<IOException> ioExceptionOptional =
                findThrowable(throwable, IOException.class);
        if (ioExceptionOptional.isPresent()) {
            return new CheckpointException(CheckpointFailureReason.IO_EXCEPTION, throwable);
        } else {
            final Optional<CheckpointException> checkpointExceptionOptional =
                    findThrowable(throwable, CheckpointException.class);
            return checkpointExceptionOptional.orElseGet(
                    () -> new CheckpointException(defaultReason, throwable));
        }
    }

    public CompletableFuture<CompletedCheckpoint> getResultFuture() {
        return pendingCheckpoint.getCompletionFuture();
    }

    static class CheckpointTriggerRequest {
        final long timestamp;
        final CheckpointProperties props;
        final @Nullable String externalSavepointLocation;
        final boolean isPeriodic;
        private final CompletableFuture<CompletedCheckpoint> onCompletionPromise =
                new CompletableFuture<>();

        CheckpointTriggerRequest(
                CheckpointProperties props,
                @Nullable String externalSavepointLocation,
                boolean isPeriodic) {

            this.timestamp = System.currentTimeMillis();
            this.props = checkNotNull(props);
            this.externalSavepointLocation = externalSavepointLocation;
            this.isPeriodic = isPeriodic;
        }

        CompletableFuture<CompletedCheckpoint> getOnCompletionFuture() {
            return onCompletionPromise;
        }

        public void completeExceptionally(CheckpointException exception) {
            onCompletionPromise.completeExceptionally(exception);
        }

        public boolean isForce() {
            return props.forceCheckpoint();
        }
    }

    private enum OperatorCoordinatorRestoreBehavior {

        /** Coordinators are always restored. If there is no checkpoint, they are restored empty. */
        RESTORE_OR_RESET,

        /** Coordinators are restored if there was a checkpoint. */
        RESTORE_IF_CHECKPOINT_PRESENT,

        /** Coordinators are not restored during this checkpoint restore. */
        SKIP;
    }

    private PendingCheckpointStats trackPendingCheckpointStats(
            long checkpointId, CheckpointProperties props, long checkpointTimestamp) {
        Map<JobVertexID, Integer> vertices =
                pendingCheckpoint.getExecutionJobVertices().stream()
                        .collect(
                                toMap(
                                        ExecutionJobVertex::getJobVertexId,
                                        ExecutionJobVertex::getParallelism));

        PendingCheckpointStats pendingCheckpointStats =
                statsTracker.reportPendingCheckpoint(
                        checkpointId, checkpointTimestamp, props, vertices);

        reportFinishedTasks(pendingCheckpointStats, pendingCheckpoint.getFinishedTasks());

        return pendingCheckpointStats;
    }

    private void reportFinishedTasks(
            @Nullable PendingCheckpointStats pendingCheckpointStats,
            List<Execution> finishedTasks) {
        if (pendingCheckpointStats == null) {
            return;
        }

        long now = System.currentTimeMillis();
        finishedTasks.forEach(
                execution ->
                        pendingCheckpointStats.reportSubtaskStats(
                                execution.getVertex().getJobvertexId(),
                                new SubtaskStateStats(execution.getParallelSubtaskIndex(), now)));
    }

    @Nullable
    private PendingCheckpointStats getStatsCallback(
            BoundedExecutionPendingCheckpoint pendingCheckpoint) {
        return statsTracker.getPendingCheckpointStats(pendingCheckpoint.getCheckpointID());
    }
}
