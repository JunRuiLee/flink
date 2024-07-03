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

package org.apache.flink.runtime.client;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.blob.BlobClient;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.util.StreamGraphTestUtils;
import org.apache.flink.testutils.junit.utils.TempDirUtils;
import org.apache.flink.util.InstantiationUtil;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ClientUtils}. */
public class ClientUtilsTest {

    @TempDir private static java.nio.file.Path temporaryFolder;

    private static BlobServer blobServer = null;

    @BeforeAll
    static void setup() throws IOException {
        Configuration config = new Configuration();
        blobServer =
                new BlobServer(
                        config, TempDirUtils.newFolder(temporaryFolder), new VoidBlobStore());
        blobServer.start();
    }

    @AfterAll
    static void teardown() throws IOException {
        if (blobServer != null) {
            blobServer.close();
        }
    }

    @Test
    void uploadAndSetUserJars() throws Exception {
        java.nio.file.Path tmpDir = TempDirUtils.newFolder(temporaryFolder).toPath();
        StreamGraph streamGraph = StreamGraphTestUtils.emptyStreamGraph();

        Collection<Path> jars =
                Arrays.asList(
                        new Path(Files.createFile(tmpDir.resolve("jar1.jar")).toString()),
                        new Path(Files.createFile(tmpDir.resolve("jar2.jar")).toString()));

        jars.forEach(streamGraph::addJar);

        assertThat(streamGraph.getUserJars()).hasSameSizeAs(jars);
        assertThat(streamGraph.getUserJarBlobKeys()).isEmpty();

        ClientUtils.extractAndUploadStreamGraphFiles(
                streamGraph,
                () ->
                        new BlobClient(
                                new InetSocketAddress("localhost", blobServer.getPort()),
                                new Configuration()));

        assertThat(streamGraph.getUserJars()).hasSameSizeAs(jars);
        assertThat(streamGraph.getUserJarBlobKeys()).hasSameSizeAs(jars);
        assertThat(streamGraph.getUserJarBlobKeys().stream().distinct()).hasSameSizeAs(jars);

        for (PermanentBlobKey blobKey : streamGraph.getUserJarBlobKeys()) {
            blobServer.getFile(streamGraph.getJobId(), blobKey);
        }
    }

    @Test
    void uploadAndSetUserArtifacts() throws Exception {
        java.nio.file.Path tmpDir = TempDirUtils.newFolder(temporaryFolder).toPath();
        StreamGraph streamGraph = StreamGraphTestUtils.emptyStreamGraph();

        Collection<DistributedCache.DistributedCacheEntry> localArtifacts =
                Arrays.asList(
                        new DistributedCache.DistributedCacheEntry(
                                Files.createFile(tmpDir.resolve("art1")).toString(), true, true),
                        new DistributedCache.DistributedCacheEntry(
                                Files.createFile(tmpDir.resolve("art2")).toString(), true, false),
                        new DistributedCache.DistributedCacheEntry(
                                Files.createFile(tmpDir.resolve("art3")).toString(), false, true),
                        new DistributedCache.DistributedCacheEntry(
                                Files.createFile(tmpDir.resolve("art4")).toString(), true, false));

        Collection<DistributedCache.DistributedCacheEntry> distributedArtifacts =
                Collections.singletonList(
                        new DistributedCache.DistributedCacheEntry(
                                "hdfs://localhost:1234/test", true, false));

        for (DistributedCache.DistributedCacheEntry entry : localArtifacts) {
            streamGraph.addUserArtifact(entry.filePath, entry);
        }
        for (DistributedCache.DistributedCacheEntry entry : distributedArtifacts) {
            streamGraph.addUserArtifact(entry.filePath, entry);
        }

        final int totalNumArtifacts = localArtifacts.size() + distributedArtifacts.size();

        assertThat(streamGraph.getUserArtifacts()).hasSize(totalNumArtifacts);
        assertThat(
                        streamGraph.getUserArtifacts().values().stream()
                                .filter(entry -> entry.blobKey != null))
                .isEmpty();

        ClientUtils.extractAndUploadStreamGraphFiles(
                streamGraph,
                () ->
                        new BlobClient(
                                new InetSocketAddress("localhost", blobServer.getPort()),
                                new Configuration()));

        assertThat(streamGraph.getUserArtifacts()).hasSize(totalNumArtifacts);
        assertThat(
                        streamGraph.getUserArtifacts().values().stream()
                                .filter(entry -> entry.blobKey != null))
                .hasSameSizeAs(localArtifacts);
        assertThat(
                        streamGraph.getUserArtifacts().values().stream()
                                .filter(entry -> entry.blobKey == null))
                .hasSameSizeAs(distributedArtifacts);
        // 1 unique key for each local artifact, and null for distributed artifacts
        assertThat(
                        streamGraph.getUserArtifacts().values().stream()
                                .map(entry -> entry.blobKey)
                                .distinct())
                .hasSize(localArtifacts.size() + 1);
        for (DistributedCache.DistributedCacheEntry original : localArtifacts) {
            assertState(
                    original,
                    streamGraph.getUserArtifacts().get(original.filePath),
                    false,
                    streamGraph.getJobId());
        }
        for (DistributedCache.DistributedCacheEntry original : distributedArtifacts) {
            assertState(
                    original,
                    streamGraph.getUserArtifacts().get(original.filePath),
                    true,
                    streamGraph.getJobId());
        }
    }

    private static void assertState(
            DistributedCache.DistributedCacheEntry original,
            DistributedCache.DistributedCacheEntry actual,
            boolean isBlobKeyNull,
            JobID jobId)
            throws Exception {
        assertThat(actual.isZipped).isEqualTo(original.isZipped);
        assertThat(actual.isExecutable).isEqualTo(original.isExecutable);
        assertThat(actual.filePath).isEqualTo(original.filePath);
        assertThat(actual.blobKey == null).isEqualTo(isBlobKeyNull);
        if (!isBlobKeyNull) {
            blobServer.getFile(
                    jobId,
                    InstantiationUtil.<PermanentBlobKey>deserializeObject(
                            actual.blobKey, ClientUtilsTest.class.getClassLoader()));
        }
    }
}
