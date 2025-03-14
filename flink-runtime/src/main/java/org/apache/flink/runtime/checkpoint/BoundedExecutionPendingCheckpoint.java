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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorInfo;
import org.apache.flink.runtime.state.CheckpointMetadataOutputStream;
import org.apache.flink.runtime.state.CheckpointStorageLocation;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.runtime.state.StateUtil;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.FutureUtils.ConjunctFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A pending checkpoint is a checkpoint that has been started, but has not been acknowledged by all
 * tasks that need to acknowledge it. Once all tasks have acknowledged it, it becomes a {@link
 * CompletedCheckpoint}.
 *
 * <p>Note that the pending checkpoint, as well as the successful checkpoint keep the state handles
 * always as serialized values, never as actual values.
 */
@NotThreadSafe
public class BoundedExecutionPendingCheckpoint implements Checkpoint {

    private final List<ExecutionJobVertex> executionJobVertices = new ArrayList<>();

    private final List<ExecutionVertex> executionVertices = new ArrayList<>();

    private final List<Execution> finishedTasks = new ArrayList<>();

    private final Supplier<Integer> pendingOperatorsCountRetriever;

    public List<ExecutionVertex> getTasksToCommitTo() {
        return Collections.unmodifiableList(executionVertices);
    }

    public List<ExecutionJobVertex> getExecutionJobVertices() {
        return Collections.unmodifiableList(executionJobVertices);
    }

    public List<Execution> getFinishedTasks() {
        return Collections.unmodifiableList(finishedTasks);
    }

    /** Result of the {@link BoundedExecutionPendingCheckpoint#acknowledgedTasks} method. */
    public enum TaskAcknowledgeResult {
        SUCCESS, // successful acknowledge of the task
        DUPLICATE, // acknowledge message is a duplicate
        UNKNOWN, // unknown task acknowledged
        DISCARDED // pending checkpoint has been discarded
    }

    // ------------------------------------------------------------------------

    /** The PendingCheckpoint logs to the same logger as the CheckpointCoordinator. */
    private static final Logger LOG = LoggerFactory.getLogger(CheckpointCoordinator.class);

    private final Object lock = new Object();

    private final JobID jobId;

    private final long checkpointId;

    private final Map<OperatorID, OperatorState> operatorStates;

    private final Map<ExecutionAttemptID, ExecutionVertex> notYetAcknowledgedTasks;

    private final Set<OperatorID> notYetAcknowledgedOperatorCoordinators;

    /** Set of acknowledged tasks. */
    private final Set<ExecutionAttemptID> acknowledgedTasks;

    /** The checkpoint properties. */
    private final CheckpointProperties props;

    /**
     * The promise to fulfill once the checkpoint has been completed. Note that it will be completed
     * only after the checkpoint is successfully added to CompletedCheckpointStore.
     */
    private final CompletableFuture<CompletedCheckpoint> onCompletionPromise;

    @Nullable private final PendingCheckpointStats pendingCheckpointStats;

    /** Target storage location to persist the checkpoint metadata to. */
    @Nullable private CheckpointStorageLocation targetLocation;

    private int numAcknowledgedTasks;

    private boolean disposed;

    private boolean discarded;

    private volatile ScheduledFuture<?> cancellerHandle;

    private CheckpointException failureCause;

    // --------------------------------------------------------------------------------------------

    public BoundedExecutionPendingCheckpoint(
            JobID jobId,
            long checkpointId,
            CheckpointProperties props,
            CompletableFuture<CompletedCheckpoint> onCompletionPromise,
            @Nullable PendingCheckpointStats pendingCheckpointStats,
            Supplier<Integer> pendingOperatorsCountRetriever) {

        this.jobId = checkNotNull(jobId);
        this.checkpointId = checkpointId;

        this.notYetAcknowledgedTasks = new HashMap<>();

        this.props = checkNotNull(props);

        this.operatorStates = new HashMap<>();
        this.notYetAcknowledgedOperatorCoordinators = new HashSet<>();
        this.acknowledgedTasks = new HashSet<>();
        this.onCompletionPromise = checkNotNull(onCompletionPromise);
        this.pendingCheckpointStats = pendingCheckpointStats;
        this.pendingOperatorsCountRetriever = checkNotNull(pendingOperatorsCountRetriever);
    }

    // --------------------------------------------------------------------------------------------

    // ------------------------------------------------------------------------
    //  Properties
    // ------------------------------------------------------------------------

    public JobID getJobId() {
        return jobId;
    }

    @Override
    public long getCheckpointID() {
        return checkpointId;
    }

    public void setCheckpointTargetLocation(CheckpointStorageLocation targetLocation) {
        this.targetLocation = targetLocation;
    }

    public int getNumberOfNonAcknowledgedTasks() {
        return notYetAcknowledgedTasks.size();
    }

    public Map<OperatorID, OperatorState> getOperatorStates() {
        return operatorStates;
    }

    public boolean isFullyAcknowledged() {
        return areTasksFullyAcknowledged() && areCoordinatorsFullyAcknowledged();
    }

    boolean areCoordinatorsFullyAcknowledged() {
        return notYetAcknowledgedOperatorCoordinators.isEmpty()
                && getPendingOperatorCount() == 0
                && !disposed;
    }

    boolean areTasksFullyAcknowledged() {
        return notYetAcknowledgedTasks.isEmpty() && getPendingOperatorCount() == 0 && !disposed;
    }

    public boolean isDisposed() {
        return disposed;
    }

    CheckpointProperties getProps() {
        return props;
    }

    /**
     * Sets the handle for the canceller to this pending checkpoint. This method fails with an
     * exception if a handle has already been set.
     *
     * @return true, if the handle was set, false, if the checkpoint is already disposed;
     */
    public boolean setCancellerHandle(ScheduledFuture<?> cancellerHandle) {
        synchronized (lock) {
            if (this.cancellerHandle == null) {
                if (!disposed) {
                    this.cancellerHandle = cancellerHandle;
                    return true;
                } else {
                    return false;
                }
            } else {
                throw new IllegalStateException("A canceller handle was already set");
            }
        }
    }

    public CheckpointException getFailureCause() {
        return failureCause;
    }

    // ------------------------------------------------------------------------
    //  Progress and Completion
    // ------------------------------------------------------------------------

    /**
     * Returns the completion future.
     *
     * @return A future to the completed checkpoint
     */
    public CompletableFuture<CompletedCheckpoint> getCompletionFuture() {
        return onCompletionPromise;
    }

    public CompletedCheckpoint finalizeCheckpoint(
            CheckpointsCleaner checkpointsCleaner, Runnable postCleanup, Executor executor)
            throws IOException {

        synchronized (lock) {
            checkState(!isDisposed(), "checkpoint is discarded");
            checkState(
                    isFullyAcknowledged(),
                    "Pending checkpoint has not been fully acknowledged yet");

            // make sure we fulfill the promise with an exception if something fails
            try {
                // write out the metadata
                final CheckpointMetadata savepoint =
                        new CheckpointMetadata(
                                checkpointId,
                                operatorStates.values(),
                                Collections.emptyList(),
                                props);
                final CompletedCheckpointStorageLocation finalizedLocation;

                try (CheckpointMetadataOutputStream out =
                        targetLocation.createMetadataOutputStream()) {
                    Checkpoints.storeCheckpointMetadata(savepoint, out);
                    finalizedLocation = out.closeAndFinalizeCheckpoint();
                }

                long millis = System.currentTimeMillis();
                CompletedCheckpoint completed =
                        new CompletedCheckpoint(
                                jobId,
                                checkpointId,
                                millis,
                                millis,
                                operatorStates,
                                null,
                                props,
                                finalizedLocation,
                                toCompletedCheckpointStats(finalizedLocation));

                // mark this pending checkpoint as disposed, but do NOT drop the state
                dispose(false, checkpointsCleaner, postCleanup, executor);

                return completed;
            } catch (Throwable t) {
                onCompletionPromise.completeExceptionally(t);
                ExceptionUtils.rethrowIOException(t);
                return null; // silence the compiler
            }
        }
    }

    @Nullable
    private CompletedCheckpointStats toCompletedCheckpointStats(
            CompletedCheckpointStorageLocation finalizedLocation) {
        return pendingCheckpointStats != null
                ? pendingCheckpointStats.toCompletedCheckpointStats(
                        finalizedLocation.getExternalPointer())
                : null;
    }

    /**
     * Acknowledges the task with the given execution attempt id and the given subtask state.
     *
     * @param executionAttemptId of the acknowledged task
     * @param operatorSubtaskStates of the acknowledged task
     * @param metrics Checkpoint metrics for the stats
     * @return TaskAcknowledgeResult of the operation
     */
    public TaskAcknowledgeResult acknowledgeTask(
            ExecutionAttemptID executionAttemptId,
            TaskStateSnapshot operatorSubtaskStates,
            CheckpointMetrics metrics) {

        synchronized (lock) {
            if (disposed) {
                return TaskAcknowledgeResult.DISCARDED;
            }

            final ExecutionVertex vertex = notYetAcknowledgedTasks.remove(executionAttemptId);

            if (vertex == null) {
                if (acknowledgedTasks.contains(executionAttemptId)) {
                    return TaskAcknowledgeResult.DUPLICATE;
                } else {
                    return TaskAcknowledgeResult.UNKNOWN;
                }
            } else {
                acknowledgedTasks.add(executionAttemptId);
            }

            long ackTimestamp = System.currentTimeMillis();
            for (OperatorIDPair operatorIDPair : vertex.getJobVertex().getOperatorIDs()) {
                updateOperatorState(vertex, operatorSubtaskStates, operatorIDPair);
            }

            ++numAcknowledgedTasks;

            // publish the checkpoint statistics
            // to prevent null-pointers from concurrent modification, copy reference onto stack
            if (pendingCheckpointStats != null) {
                // Do this in millis because the web frontend works with them
                long alignmentDurationMillis = metrics.getAlignmentDurationNanos() / 1_000_000;
                long checkpointStartDelayMillis =
                        metrics.getCheckpointStartDelayNanos() / 1_000_000;

                SubtaskStateStats subtaskStateStats =
                        new SubtaskStateStats(
                                vertex.getParallelSubtaskIndex(),
                                ackTimestamp,
                                metrics.getBytesPersistedOfThisCheckpoint(),
                                metrics.getTotalBytesPersisted(),
                                metrics.getSyncDurationMillis(),
                                metrics.getAsyncDurationMillis(),
                                metrics.getBytesProcessedDuringAlignment(),
                                metrics.getBytesPersistedDuringAlignment(),
                                alignmentDurationMillis,
                                checkpointStartDelayMillis,
                                metrics.getUnalignedCheckpoint(),
                                true);

                LOG.trace(
                        "Checkpoint {} stats for {}: size={}Kb, duration={}ms, sync part={}ms, async part={}ms",
                        checkpointId,
                        vertex.getTaskNameWithSubtaskIndex(),
                        subtaskStateStats.getStateSize() == 0
                                ? 0
                                : subtaskStateStats.getStateSize() / 1024,
                        subtaskStateStats.getEndToEndDuration(
                                pendingCheckpointStats.getTriggerTimestamp()),
                        subtaskStateStats.getSyncCheckpointDuration(),
                        subtaskStateStats.getAsyncCheckpointDuration());
                pendingCheckpointStats.reportSubtaskStats(
                        vertex.getJobvertexId(), subtaskStateStats);
            }

            return TaskAcknowledgeResult.SUCCESS;
        }
    }

    private void updateOperatorState(
            ExecutionVertex vertex,
            TaskStateSnapshot operatorSubtaskStates,
            OperatorIDPair operatorIDPair) {
        OperatorState operatorState = operatorStates.get(operatorIDPair.getGeneratedOperatorID());

        if (operatorState == null) {
            operatorState =
                    new OperatorState(
                            operatorIDPair.getUserDefinedOperatorName(),
                            operatorIDPair.getUserDefinedOperatorUid(),
                            operatorIDPair.getGeneratedOperatorID(),
                            vertex.getTotalNumberOfParallelSubtasks(),
                            vertex.getMaxParallelism());
            operatorStates.put(operatorIDPair.getGeneratedOperatorID(), operatorState);
        } else {
            operatorState.setOperatorName(operatorIDPair.getUserDefinedOperatorName());
            operatorState.setOperatorUid(operatorIDPair.getUserDefinedOperatorUid());
        }
        OperatorSubtaskState operatorSubtaskState =
                operatorSubtaskStates == null
                        ? null
                        : operatorSubtaskStates.getSubtaskStateByOperatorID(
                                operatorIDPair.getGeneratedOperatorID());

        if (operatorSubtaskState != null) {
            operatorState.putState(vertex.getParallelSubtaskIndex(), operatorSubtaskState);
        }
    }

    public TaskAcknowledgeResult acknowledgeCoordinatorState(
            OperatorInfo coordinatorInfo, @Nullable ByteStreamStateHandle stateHandle) {

        synchronized (lock) {
            if (disposed) {
                return TaskAcknowledgeResult.DISCARDED;
            }

            final OperatorID operatorId = coordinatorInfo.operatorId();
            OperatorState operatorState = operatorStates.get(operatorId);

            // sanity check for better error reporting
            if (!notYetAcknowledgedOperatorCoordinators.remove(operatorId)) {
                return operatorState != null && operatorState.getCoordinatorState() != null
                        ? TaskAcknowledgeResult.DUPLICATE
                        : TaskAcknowledgeResult.UNKNOWN;
            }

            if (operatorState == null) {
                operatorState =
                        new OperatorState(
                                null,
                                null,
                                operatorId,
                                coordinatorInfo.currentParallelism(),
                                coordinatorInfo.maxParallelism());
                operatorStates.put(operatorId, operatorState);
            }
            if (stateHandle != null) {
                operatorState.setCoordinatorState(stateHandle);
            }

            return TaskAcknowledgeResult.SUCCESS;
        }
    }

    // ------------------------------------------------------------------------
    //  Cancellation
    // ------------------------------------------------------------------------

    /** Aborts a checkpoint with reason and cause. */
    public void abort(
            CheckpointFailureReason reason,
            @Nullable Throwable cause,
            CheckpointsCleaner checkpointsCleaner,
            Runnable postCleanup,
            Executor executor) {
        try {
            failureCause = new CheckpointException(reason, cause);
            onCompletionPromise.completeExceptionally(failureCause);
            assertAbortSubsumedForced(reason);
        } finally {
            dispose(true, checkpointsCleaner, postCleanup, executor);
        }
    }

    private void assertAbortSubsumedForced(CheckpointFailureReason reason) {
        if (props.isSavepoint() && reason == CheckpointFailureReason.CHECKPOINT_SUBSUMED) {
            throw new IllegalStateException(
                    "Bug: savepoints must never be subsumed, "
                            + "the abort reason is : "
                            + reason.message());
        }
    }

    private void dispose(
            boolean releaseState,
            CheckpointsCleaner checkpointsCleaner,
            Runnable postCleanup,
            Executor executor) {

        synchronized (lock) {
            try {
                numAcknowledgedTasks = -1;
                checkpointsCleaner.cleanCheckpoint(this, releaseState, postCleanup, executor);
            } finally {
                disposed = true;
                notYetAcknowledgedTasks.clear();
                acknowledgedTasks.clear();
                cancelCanceller();
            }
        }
    }

    @Override
    public DiscardObject markAsDiscarded() {
        return new PendingCheckpointDiscardObject();
    }

    private void cancelCanceller() {
        try {
            final ScheduledFuture<?> canceller = this.cancellerHandle;
            if (canceller != null) {
                canceller.cancel(false);
            }
        } catch (Exception e) {
            // this code should not throw exceptions
            LOG.warn("Error while cancelling checkpoint timeout task", e);
        }
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    @Override
    public String toString() {
        return String.format(
                "Pending Checkpoint %d @ - confirmed=%d, nonAcknowledgedTasks=%d, pendingOperators=",
                checkpointId,
                numAcknowledgedTasks,
                getNumberOfNonAcknowledgedTasks(),
                getPendingOperatorCount());
    }

    private int getPendingOperatorCount() {
        return pendingOperatorsCountRetriever.get();
    }

    /** Implementation of {@link DiscardObject} for {@link BoundedExecutionPendingCheckpoint}. */
    public class PendingCheckpointDiscardObject implements DiscardObject {
        /**
         * Discard state. Must be called after {@link #dispose(boolean, CheckpointsCleaner,
         * Runnable, Executor) dispose}.
         */
        @Override
        public void discard() {
            synchronized (lock) {
                if (discarded) {
                    Preconditions.checkState(
                            disposed, "Checkpoint should be disposed before being discarded");
                    return;
                } else {
                    discarded = true;
                }
            }
            // discard the private states.
            // unregistered shared states are still considered private at this point.
            try {
                StateUtil.bestEffortDiscardAllStateObjects(operatorStates.values());
                if (targetLocation != null) {
                    targetLocation.disposeOnFailure();
                }
            } catch (Throwable t) {
                LOG.warn(
                        "Could not properly dispose the private states in the pending checkpoint {} of job {}.",
                        checkpointId,
                        jobId,
                        t);
            } finally {
                operatorStates.clear();
            }
        }

        @Override
        public CompletableFuture<Void> discardAsync(Executor ioExecutor) {
            synchronized (lock) {
                if (discarded) {
                    Preconditions.checkState(
                            disposed, "Checkpoint should be disposed before being discarded");
                } else {
                    discarded = true;
                }
            }
            List<StateObject> discardables =
                    operatorStates.values().stream()
                            .flatMap(op -> op.getDiscardables().stream())
                            .collect(Collectors.toList());

            ConjunctFuture<Void> discardStates =
                    FutureUtils.completeAll(
                            discardables.stream()
                                    .map(
                                            item ->
                                                    FutureUtils.runAsync(
                                                            item::discardState, ioExecutor))
                                    .collect(Collectors.toList()));

            return FutureUtils.runAfterwards(
                    discardStates,
                    () -> {
                        operatorStates.clear();
                        if (targetLocation != null) {
                            targetLocation.disposeOnFailure();
                        }
                    });
        }
    }

    public void onJobVertexInitialized(ExecutionJobVertex executionJobVertex) {
        checkNotNull(executionJobVertex);
        checkState(executionJobVertex.isInitialized());

        this.executionJobVertices.add(executionJobVertex);
        for (ExecutionVertex taskVertex : executionJobVertex.getTaskVertices()) {
            this.notYetAcknowledgedTasks.put(
                    taskVertex.getCurrentExecutionAttempt().getAttemptId(), taskVertex);
            this.executionVertices.add(taskVertex);
        }
    }

    public void onJobVertexFinished(ExecutionJobVertex executionJobVertex) {
        checkNotNull(executionJobVertex);
        checkState(executionJobVertex.isFinished());

        for (ExecutionVertex taskVertex : executionJobVertex.getTaskVertices()) {
            finishedTasks.add(taskVertex.getCurrentExecutionAttempt());
        }
        executionJobVertex
                .getOperatorCoordinators()
                .forEach(
                        operatorCoordinator ->
                                notYetAcknowledgedOperatorCoordinators.add(
                                        operatorCoordinator.operatorId()));
    }
}
