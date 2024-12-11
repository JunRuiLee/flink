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

package org.apache.flink.runtime.shuffle;

import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.InstantiationUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;

/**
 * Utility class for handling cluster-level snapshots for the ShuffleMaster. This class provides
 * methods to write snapshots to the file system and read snapshots from the file system. Snapshots
 * are immutable and these operations should be called only during the startup phase of a Flink
 * cluster.
 *
 * <p>Snapshots are written to and read from files in the specified working directory. The files
 * created are named using a prefix followed by the cluster ID.
 */
public class ShuffleMasterSnapshotUtil {

    private static final String FILE_PREFIX = "shuffle_master_snapshot_";
    private static final Logger LOG = LoggerFactory.getLogger(ShuffleMasterSnapshotUtil.class);

    /**
     * Writes an immutable snapshot of the ShuffleMaster to the specified directory. This method
     * should be called only during the startup phase of the Flink cluster.
     *
     * @param snapshot The snapshot data to be written.
     * @param workingDir The directory where the snapshot file will be written.
     * @param clusterId The unique identifier for the cluster.
     * @throws IOException If an I/O error occurs while writing the snapshot.
     */
    public static void writeSnapshot(
            ShuffleMasterSnapshot snapshot, Path workingDir, String clusterId) throws IOException {
        FileSystem fileSystem = workingDir.getFileSystem();
        if (fileSystem.exists(workingDir)) {
            throw new IOException("Shuffle master dir " + workingDir + " already exists.");
        }

        fileSystem.mkdirs(workingDir);
        LOG.info("Create shuffle master snapshot dir {}.", workingDir);

        Path writeFile = new Path(workingDir, FILE_PREFIX + clusterId);
        try (FSDataOutputStream outputStream =
                fileSystem.create(writeFile, FileSystem.WriteMode.NO_OVERWRITE)) {
            byte[] bytes = InstantiationUtil.serializeObject(snapshot);
            writeInt(outputStream, bytes.length);
            outputStream.write(bytes);
        }
    }

    private static void writeInt(FSDataOutputStream outputStream, int num) throws IOException {
        outputStream.write((num >>> 24) & 0xFF);
        outputStream.write((num >>> 16) & 0xFF);
        outputStream.write((num >>> 8) & 0xFF);
        outputStream.write((num) & 0xFF);
    }

    /**
     * Checks if a ShuffleMaster snapshot exists for the specified cluster.
     *
     * @param workingDir The directory where the snapshot file is expected.
     * @param clusterId The unique identifier for the cluster.
     * @return True if the snapshot file exists, false otherwise.
     * @throws IOException If an I/O error occurs while checking the file existence.
     */
    public static boolean isShuffleMasterSnapshotExist(Path workingDir, String clusterId)
            throws IOException {
        FileSystem fileSystem = workingDir.getFileSystem();
        return fileSystem.exists(new Path(workingDir, FILE_PREFIX + clusterId));
    }

    /**
     * Reads an immutable snapshot of the ShuffleMaster from the specified directory. This method
     * should be called only during the startup phase of the Flink cluster.
     *
     * @param workingDir The directory where the snapshot file is located.
     * @param clusterId The unique identifier for the cluster.
     * @return The snapshot data read from the file.
     * @throws IOException If an I/O error occurs while reading the snapshot.
     */
    public static ShuffleMasterSnapshot readSnapshot(Path workingDir, String clusterId)
            throws IOException {
        FileSystem fileSystem = workingDir.getFileSystem();
        Path file = new Path(workingDir, FILE_PREFIX + clusterId);
        try (DataInputStream inputStream = new DataInputStream(fileSystem.open(file))) {
            int byteLength = inputStream.readInt();
            byte[] bytes = new byte[byteLength];
            inputStream.readFully(bytes);
            return InstantiationUtil.deserializeObject(bytes, ClassLoader.getSystemClassLoader());
        } catch (ClassNotFoundException exception) {
            throw new IOException("Deserialize ShuffleMasterSnapshot failed.", exception);
        }
    }
}
