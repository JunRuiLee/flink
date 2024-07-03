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
import org.apache.flink.streaming.api.graph.StreamGraph;

import javax.annotation.Nullable;

import java.util.Collection;

/** {@link StreamGraph} instances for recovery. */
public interface StreamGraphStore extends StreamGraphWriter {

    /** Starts the {@link StreamGraphStore} service. */
    void start(StreamGraphListener streamGraphListener) throws Exception;

    /** Stops the {@link StreamGraphStore} service. */
    void stop() throws Exception;

    /**
     * Returns the {@link StreamGraph} with the given {@link JobID} or {@code null} if no job was
     * registered.
     */
    @Nullable
    StreamGraph recoverStreamGraph(JobID jobId) throws Exception;

    /**
     * Get all job ids of submitted job graphs to the submitted job graph store.
     *
     * @return Collection of submitted job ids
     * @throws Exception if the operation fails
     */
    Collection<JobID> getJobIds() throws Exception;

    /**
     * A listener for {@link StreamGraph} instances. This is used to react to races between multiple
     * running {@link StreamGraphStore} instances (on multiple job managers).
     */
    interface StreamGraphListener {

        /**
         * Callback for {@link StreamGraph} instances added by a different {@link StreamGraphStore}
         * instance.
         *
         * <p><strong>Important:</strong> It is possible to get false positives and be notified
         * about a job graph, which was added by this instance.
         *
         * @param jobId The {@link JobID} of the added job graph
         */
        void onAddedStreamGraph(JobID jobId);

        /**
         * Callback for {@link StreamGraph} instances removed by a different {@link
         * StreamGraphStore} instance.
         *
         * @param jobId The {@link JobID} of the removed job graph
         */
        void onRemovedStreamGraph(JobID jobId);
    }
}
