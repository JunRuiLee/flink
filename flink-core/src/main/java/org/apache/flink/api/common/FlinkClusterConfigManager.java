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

package org.apache.flink.api.common;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.FallbackKey;
import org.apache.flink.core.fs.FileSystemFactory;
import org.apache.flink.core.plugin.PluginLoader;
import org.apache.flink.metrics.reporter.MetricReporterFactory;
import org.apache.flink.util.IterableUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Options which takes effect at the cluster-level. */
public class FlinkClusterConfigManager {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkClusterConfigManager.class);

    // The collection of cluster config options which can be set be users
    private static final Set<String> CORE_OPTIONS_CLASSES =
            new HashSet<>(
                    Arrays.asList(
                            "org.apache.flink.changelog.fs.FsStateChangelogOptions",
                            "org.apache.flink.client.cli.ClientOptions",
                            "org.apache.flink.client.deployment.application.ApplicationConfiguration",
                            "org.apache.flink.configuration.AkkaOptions",
                            "org.apache.flink.configuration.AlgorithmOptions",
                            "org.apache.flink.configuration.BatchExecutionOptions",
                            "org.apache.flink.configuration.BlobServerOptions",
                            "org.apache.flink.configuration.CleanupOptions",
                            "org.apache.flink.configuration.ClusterOptions",
                            "org.apache.flink.configuration.CoreOptions",
                            "org.apache.flink.configuration.DeploymentOptions",
                            "org.apache.flink.configuration.ExternalResourceOptions",
                            "org.apache.flink.configuration.HeartbeatManagerOptions",
                            "org.apache.flink.configuration.HighAvailabilityOptions",
                            "org.apache.flink.configuration.HistoryServerOptions",
                            "org.apache.flink.configuration.JMXServerOptions",
                            "org.apache.flink.configuration.JobManagerOptions",
                            "org.apache.flink.configuration.MetricOptions",
                            "org.apache.flink.configuration.NettyShuffleEnvironmentOptions",
                            "org.apache.flink.configuration.OptimizerOptions",
                            "org.apache.flink.configuration.QueryableStateOptions",
                            "org.apache.flink.configuration.ResourceManagerOptions",
                            "org.apache.flink.configuration.RestOptions",
                            "org.apache.flink.configuration.SecurityOptions",
                            "org.apache.flink.configuration.SlowTaskDetectorOptions",
                            "org.apache.flink.configuration.TaskManagerOptions",
                            "org.apache.flink.configuration.WebOptions",
                            "org.apache.flink.kubernetes.configuration.KubernetesConfigOptions",
                            "org.apache.flink.kubernetes.configuration.KubernetesHighAvailabilityOptions",
                            "org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory",
                            "org.apache.flink.runtime.dispatcher.Dispatcher",
                            "org.apache.flink.runtime.entrypoint.ClusterEntrypoint",
                            "org.apache.flink.runtime.entrypoint.component.FileJobGraphRetriever",
                            "org.apache.flink.runtime.highavailability.JobResultStoreOptions",
                            "org.apache.flink.runtime.jobgraph.SavepointConfigOptions",
                            "org.apache.flink.runtime.shuffle.ShuffleServiceOptions",
                            "org.apache.flink.runtime.util.config.memory.JvmMetaspaceAndOverheadOptions",
                            "org.apache.flink.runtime.util.config.memory.LegacyMemoryOptions",
                            "org.apache.flink.streaming.api.operators.collect.CollectSinkOperatorFactory",
                            "org.apache.flink.yarn.configuration.YarnConfigOptions"));

    private static final Set<String> FS_PLUGINS_OPTIONS =
            new HashSet<>(
                    Arrays.asList(
                            "org.apache.flink.fs.gs.GSFileSystemOptions",
                            "org.apache.flink.fs.osshadoop.OSSFileSystemFactory",
                            "org.apache.flink.fs.s3.common.AbstractS3FileSystemFactory"));

    private static final Set<String> METRIC_REPORTER_PLUGINS_OPTIONS =
            new HashSet<>(
                    Arrays.asList(
                            "org.apache.flink.metrics.influxdb.InfluxdbReporterOptions",
                            "org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporterOptions"));

    private static final Set<String> CLUSTER_OPTIONS_EXCLUDED =
            new HashSet<>(
                    Arrays.asList(
                            "org.apache.flink.configuration.DeploymentOptions#JOB_LISTENERS",
                            "org.apache.flink.configuration.BatchExecutionOptions#ADAPTIVE_AUTO_PARALLELISM_ENABLED",
                            "org.apache.flink.configuration.DeploymentOptions#TARGET",
                            "org.apache.flink.configuration.DeploymentOptions#ATTACHED",
                            "org.apache.flink.configuration.MetricOptions#LATENCY_INTERVAL",
                            "org.apache.flink.configuration.TaskManagerOptions#TASK_CANCELLATION_INTERVAL",
                            "org.apache.flink.configuration.TaskManagerOptions#TASK_CANCELLATION_TIMEOUT",
                            "org.apache.flink.configuration.JobManagerOptions#SCHEDULER"));
    private static final Set<String> CLUSTER_OPTIONS_INCLUDED =
            new HashSet<>(
                    Arrays.asList(
                            "org.apache.flink.configuration.StateChangelogOptions#STATE_CHANGE_LOG_STORAGE"));

    private static Set<String> managedClusterOptionsKeys;

    private static Set<String> registeredClusterOptionsKeys = new HashSet<>();

    public static void attemptAddPluginOptionKeys(
            Class<?> service, PluginLoader pluginLoader, String pluginId) {
        checkNotNull(service);
        if (service.equals(FileSystemFactory.class)) {
            attemptAddPluginOptionKeys(FS_PLUGINS_OPTIONS, pluginLoader, pluginId);
        } else if (service.equals(MetricReporterFactory.class)) {
            attemptAddPluginOptionKeys(METRIC_REPORTER_PLUGINS_OPTIONS, pluginLoader, pluginId);
        }
    }

    private static void attemptAddPluginOptionKeys(
            Set<String> pluginOptionClasses, PluginLoader pluginLoader, String pluginId) {
        for (String className : pluginOptionClasses) {
            try {
                Class<?> clazz = pluginLoader.load(className);
                addClusterOptionKeys(clazz, registeredClusterOptionsKeys);
            } catch (Exception e) {
                LOG.debug("Failed load {} by using {}", className, pluginId);
            }
        }
    }

    private static void addClusterOptionKeys(
            Class<?> clazz, Set<String> set, String... targetFields) {
        try {
            String className = clazz.getName();
            if (targetFields.length == 0) {
                for (Field field : clazz.getDeclaredFields()) {
                    if (!CLUSTER_OPTIONS_EXCLUDED.contains(className + "#" + field.getName())) {
                        addConfigOptionKey(field, set);
                    }
                }
            } else {
                for (String fieldName : targetFields) {
                    if (!CLUSTER_OPTIONS_EXCLUDED.contains(className + "#" + fieldName)) {
                        addConfigOptionKey(clazz.getDeclaredField(fieldName), set);
                    }
                }
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private static void addConfigOptionKey(Field field, Set<String> set) throws Exception {
        if (Modifier.isStatic(field.getModifiers())
                && Modifier.isPublic(field.getModifiers())
                && field.getType() == ConfigOption.class) {
            ConfigOption<?> configOption = (ConfigOption<?>) field.get(null);

            Method keyMethod = ConfigOption.class.getDeclaredMethod("key");
            Method fallbackKeysMethod = ConfigOption.class.getDeclaredMethod("fallbackKeys");
            set.add((String) keyMethod.invoke(configOption));
            Iterable<FallbackKey> iterable =
                    (Iterable<FallbackKey>) fallbackKeysMethod.invoke(configOption);
            IterableUtils.toStream(iterable).forEach(fallbackKey -> set.add(fallbackKey.getKey()));
        }
    }

    public static Map<String, String> filterJobConfiguration(Map<String, String> config) {
        if (managedClusterOptionsKeys == null) {
            synchronized (FlinkClusterConfigManager.class) {
                if (managedClusterOptionsKeys == null) {
                    try {
                        managedClusterOptionsKeys = new HashSet<>();
                        for (String className : CORE_OPTIONS_CLASSES) {
                            Class<?> clazz = Class.forName(className);
                            addClusterOptionKeys(clazz, managedClusterOptionsKeys);
                        }

                        for (String s : CLUSTER_OPTIONS_INCLUDED) {
                            String[] split = s.split("#");
                            Class<?> clazz = Class.forName(split[0]);
                            addClusterOptionKeys(clazz, managedClusterOptionsKeys, split[1]);
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        return config.entrySet().stream()
                .filter(
                        entry ->
                                !managedClusterOptionsKeys.contains(entry.getKey())
                                        && !registeredClusterOptionsKeys.contains(entry.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
