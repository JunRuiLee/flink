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

package org.apache.flink.runtime.dispatcher.runner;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.core.testutils.AllCallbackWrapper;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.BlobUtils;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.dispatcher.DispatcherId;
import org.apache.flink.runtime.dispatcher.DispatcherOperationCaches;
import org.apache.flink.runtime.dispatcher.MemoryExecutionGraphInfoStore;
import org.apache.flink.runtime.dispatcher.PartialDispatcherServices;
import org.apache.flink.runtime.dispatcher.SessionDispatcherFactory;
import org.apache.flink.runtime.dispatcher.VoidHistoryServerArchivist;
import org.apache.flink.runtime.heartbeat.TestingHeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.highavailability.JobResultStore;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServicesBuilder;
import org.apache.flink.runtime.highavailability.nonha.embedded.EmbeddedJobResultStore;
import org.apache.flink.runtime.highavailability.zookeeper.CuratorFrameworkWithUnhandledErrorListener;
import org.apache.flink.runtime.jobmanager.JobPersistenceComponentFactory;
import org.apache.flink.runtime.jobmanager.StreamGraphStore;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.leaderelection.LeaderElection;
import org.apache.flink.runtime.leaderelection.TestingLeaderElection;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.rpc.TestingRpcServiceExtension;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.runtime.zookeeper.ZooKeeperExtension;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.util.StreamGraphTestUtils;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.testutils.junit.utils.TempDirUtils;
import org.apache.flink.util.ExceptionUtils;

import org.apache.flink.shaded.curator5.org.apache.curator.framework.CuratorFramework;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the interaction between the {@link DefaultDispatcherRunner} and ZooKeeper. */
class ZooKeeperDefaultDispatcherRunnerTest {

    private static final Logger LOG =
            LoggerFactory.getLogger(ZooKeeperDefaultDispatcherRunnerTest.class);

    private static final Time TESTING_TIMEOUT = Time.seconds(10L);

    @RegisterExtension
    public static AllCallbackWrapper<ZooKeeperExtension> zooKeeperExtensionWrapper =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    @TempDir public static Path temporaryFolder;

    @RegisterExtension
    public static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_EXTENSION =
            TestingUtils.defaultExecutorExtension();

    @RegisterExtension
    public static AllCallbackWrapper<TestingRpcServiceExtension> testingRpcServiceExtensionWrapper =
            new AllCallbackWrapper<>(new TestingRpcServiceExtension());

    private BlobServer blobServer;

    private TestingFatalErrorHandler fatalErrorHandler;

    private File clusterHaStorageDir;

    private Configuration configuration;

    @BeforeEach
    void setup() throws IOException {
        fatalErrorHandler = new TestingFatalErrorHandler();
        configuration = new Configuration();
        configuration.set(HighAvailabilityOptions.HA_MODE, "zookeeper");
        configuration.set(
                HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM,
                zooKeeperExtensionWrapper.getCustomExtension().getConnectString());
        configuration.set(
                HighAvailabilityOptions.HA_STORAGE_PATH,
                TempDirUtils.newFolder(temporaryFolder).getAbsolutePath());

        clusterHaStorageDir =
                new File(
                        HighAvailabilityServicesUtils.getClusterHighAvailableStoragePath(
                                        configuration)
                                .toString());
        blobServer =
                new BlobServer(
                        configuration,
                        TempDirUtils.newFolder(temporaryFolder),
                        BlobUtils.createBlobStoreFromConfig(configuration));
    }

    @AfterEach
    void teardown() throws Exception {
        if (blobServer != null) {
            blobServer.close();
        }

        if (fatalErrorHandler != null) {
            fatalErrorHandler.rethrowError();
        }
    }

    /** See FLINK-11665. */
    @Test
    void testResourceCleanupUnderLeadershipChange() throws Exception {
        final TestingRpcService rpcService =
                testingRpcServiceExtensionWrapper.getCustomExtension().getTestingRpcService();
        final TestingLeaderElection dispatcherLeaderElection = new TestingLeaderElection();

        try (final CuratorFrameworkWithUnhandledErrorListener curatorFrameworkWrapper =
                        ZooKeeperUtils.startCuratorFramework(configuration, fatalErrorHandler);
                final TestingHighAvailabilityServices highAvailabilityServices =
                        new TestingHighAvailabilityServicesBuilder()
                                .setDispatcherLeaderElection(dispatcherLeaderElection)
                                .setJobMasterLeaderRetrieverFunction(
                                        jobId ->
                                                ZooKeeperUtils.createLeaderRetrievalService(
                                                        curatorFrameworkWrapper
                                                                .asCuratorFramework()))
                                .build()) {

            final PartialDispatcherServices partialDispatcherServices =
                    new PartialDispatcherServices(
                            configuration,
                            highAvailabilityServices,
                            CompletableFuture::new,
                            blobServer,
                            new TestingHeartbeatServices(),
                            UnregisteredMetricGroups::createUnregisteredJobManagerMetricGroup,
                            new MemoryExecutionGraphInfoStore(),
                            fatalErrorHandler,
                            VoidHistoryServerArchivist.INSTANCE,
                            null,
                            ForkJoinPool.commonPool(),
                            new DispatcherOperationCaches(),
                            Collections.emptySet());

            final DefaultDispatcherRunnerFactory defaultDispatcherRunnerFactory =
                    DefaultDispatcherRunnerFactory.createSessionRunner(
                            SessionDispatcherFactory.INSTANCE);

            try (final DispatcherRunner dispatcherRunner =
                    createDispatcherRunner(
                            rpcService,
                            dispatcherLeaderElection,
                            new JobPersistenceComponentFactory() {
                                @Override
                                public StreamGraphStore createStreamGraphStore() {
                                    return createZooKeeperStreamGraphStore(
                                            curatorFrameworkWrapper.asCuratorFramework());
                                }

                                @Override
                                public JobResultStore createJobResultStore() {
                                    return new EmbeddedJobResultStore();
                                }
                            },
                            partialDispatcherServices,
                            defaultDispatcherRunnerFactory)) {

                // initial run
                DispatcherGateway dispatcherGateway = grantLeadership(dispatcherLeaderElection);

                final StreamGraph streamGraph = createStreamGraphWithBlobs();
                LOG.info("Initial job submission {}.", streamGraph.getJobId());
                dispatcherGateway.submitJob(streamGraph, TESTING_TIMEOUT).get();

                dispatcherLeaderElection.notLeader();

                // recovering submitted jobs
                LOG.info("Re-grant leadership first time.");
                dispatcherGateway = grantLeadership(dispatcherLeaderElection);

                LOG.info("Cancel recovered job {}.", streamGraph.getJobId());
                // cancellation of the job should remove everything
                final CompletableFuture<JobResult> jobResultFuture =
                        dispatcherGateway.requestJobResult(streamGraph.getJobId(), TESTING_TIMEOUT);
                dispatcherGateway.cancelJob(streamGraph.getJobId(), TESTING_TIMEOUT).get();

                // a successful cancellation should eventually remove all job information
                final JobResult jobResult = jobResultFuture.get();

                assertThat(jobResult.getApplicationStatus()).isEqualTo(ApplicationStatus.CANCELED);

                dispatcherLeaderElection.notLeader();

                // check that the job has been removed from ZooKeeper
                final StreamGraphStore submittedStreamGraphStore =
                        createZooKeeperStreamGraphStore(
                                curatorFrameworkWrapper.asCuratorFramework());

                CommonTestUtils.waitUntilCondition(
                        () -> submittedStreamGraphStore.getJobIds().isEmpty(), 20L);
            }
        }

        // check resource clean up
        assertThat(clusterHaStorageDir).isEmptyDirectory();
    }

    private DispatcherRunner createDispatcherRunner(
            TestingRpcService rpcService,
            LeaderElection dispatcherLeaderElection,
            JobPersistenceComponentFactory jobPersistenceComponentFactory,
            PartialDispatcherServices partialDispatcherServices,
            DispatcherRunnerFactory dispatcherRunnerFactory)
            throws Exception {
        return dispatcherRunnerFactory.createDispatcherRunner(
                dispatcherLeaderElection,
                fatalErrorHandler,
                jobPersistenceComponentFactory,
                EXECUTOR_EXTENSION.getExecutor(),
                rpcService,
                partialDispatcherServices);
    }

    private StreamGraphStore createZooKeeperStreamGraphStore(CuratorFramework client) {
        try {
            return ZooKeeperUtils.createStreamGraphs(client, configuration);
        } catch (Exception e) {
            ExceptionUtils.rethrow(e);
            return null;
        }
    }

    private DispatcherGateway grantLeadership(TestingLeaderElection dispatcherLeaderElection)
            throws InterruptedException, java.util.concurrent.ExecutionException {
        return dispatcherLeaderElection
                .isLeader(UUID.randomUUID())
                .thenCompose(
                        leaderInformation ->
                                testingRpcServiceExtensionWrapper
                                        .getCustomExtension()
                                        .getTestingRpcService()
                                        .connect(
                                                leaderInformation.getLeaderAddress(),
                                                DispatcherId.fromUuid(
                                                        leaderInformation.getLeaderSessionID()),
                                                DispatcherGateway.class))
                .get();
    }

    private StreamGraph createStreamGraphWithBlobs() throws IOException {
        final StreamGraph streamGraph = StreamGraphTestUtils.singleNoOpStreamGraph();
        final PermanentBlobKey permanentBlobKey =
                blobServer.putPermanent(streamGraph.getJobId(), new byte[256]);
        streamGraph.addUserJarBlobKey(permanentBlobKey);

        return streamGraph;
    }
}
