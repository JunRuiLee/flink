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

package org.apache.flink.configuration;

import org.apache.flink.core.plugin.PluginLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Iterator;
import java.util.ServiceLoader;
import java.util.Set;

/**
 * The {@code ConfigOptionRegistry} class manages the discovery and registration of {@link
 * ConfigOptionProvider} instances. It offers methods to automatically discover providers in the
 * system classloader and to manually register providers from other classloaders, such as plugin
 * classloaders.
 */
public class ConfigOptionRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigOptionRegistry.class);

    private static final ConfigOptionRegistry INSTANCE = new ConfigOptionRegistry();

    private final Set<ConfigOption<?>> registeredConfigOptions = new HashSet<>();

    /**
     * Instantiates a new instance of the registry and discovers available providers using the
     * system classloader.
     */
    private ConfigOptionRegistry() {
        registerConfigOptionsFromSystemClassloader();
    }

    public static ConfigOptionRegistry getInstance() {
        return INSTANCE;
    }

    /**
     * Discovers and registers config options which are defined in the {@link ConfigOptionProvider}
     * instances available in the system classloader.
     */
    private void registerConfigOptionsFromSystemClassloader() {
        LOG.info("Start registerConfigOptionsFromSystemClassloader");
        long currentTimeMillis = System.currentTimeMillis();
        ClassLoader classLoader = ClassLoader.getSystemClassLoader();
        ServiceLoader<ConfigOptionProvider> serviceLoader =
                ServiceLoader.load(ConfigOptionProvider.class, classLoader);

        serviceLoader.forEach(provider -> registeredConfigOptions.addAll(provider.options()));
        LOG.info(
                "Complete registerConfigOptionsFromSystemClassloader in {} ms, size is {}",
                System.currentTimeMillis() - currentTimeMillis,
                registeredConfigOptions.size());
    }

    /**
     * Discovers and registers config options which are defined in the {@link ConfigOptionProvider}
     * instances available in the specified PluginLoader.
     */
    public void registerConfigOptionsFromPluginLoader(PluginLoader pluginLoader) {
        LOG.info("Start registerConfigOptionsFromPluginLoader");
        long currentTimeMillis = System.currentTimeMillis();
        Iterator<ConfigOptionProvider> iterator = pluginLoader.load(ConfigOptionProvider.class);

        while (iterator.hasNext()) {
            registeredConfigOptions.addAll(iterator.next().options());
        }
        LOG.info(
                "Complete registerConfigOptionsFromSystemClassloader in {} ms, size is {}",
                System.currentTimeMillis() - currentTimeMillis,
                registeredConfigOptions.size());
    }

    public void logConfigOptionMapping() {
        LOG.info("Starting log registered config, size is {}", registeredConfigOptions.size());
        registeredConfigOptions.forEach(v -> LOG.info("Find confOption: {}", v.key()));
    }
}
