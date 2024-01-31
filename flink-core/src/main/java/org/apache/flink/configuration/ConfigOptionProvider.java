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

import java.util.Collection;

/**
 * The interface is used to mark classes that define a set of configuration options ({@link
 * org.apache.flink.configuration.ConfigOption}). Implementing this interface allows for the
 * discovery of available configuration options via SPI mechanism, facilitating the aggregation and
 * management of Flink configuration properties.
 *
 * <p>Note: Implementing classes must be registered in the {@code
 * META-INF/services/org.apache.flink.configuration.ConfigOptionsProvider} file within the resource
 * directory of their respective module to be discoverable via SPI.
 */
public interface ConfigOptionProvider {
    /**
     * Retrieves a set of configuration options defined by the implementing class.
     *
     * @return a set of {@link org.apache.flink.configuration.ConfigOption} objects
     */
    Collection<ConfigOption<?>> options();
}
