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

package org.apache.flink.runtime.scheduler.adaptivebatch;

import org.apache.flink.configuration.BatchExecutionOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.graph.StreamGraphContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class StreamGraphOptimizer {

    private final List<StreamGraphOptimizationStrategy> strategies;

    private static final Logger LOG = LoggerFactory.getLogger(StreamGraphOptimizer.class);

    public StreamGraphOptimizer(Configuration configuration) {
        this.strategies = loadStrategiesFromConfig(configuration);
    }

    private List<StreamGraphOptimizationStrategy> loadStrategiesFromConfig(
            Configuration configuration) {
        List<StreamGraphOptimizationStrategy> strategies = new ArrayList<>();

        List<String> strategyClassNames =
                configuration.get(
                        BatchExecutionOptions.ADAPTIVE_STREAM_GRAPH_OPTIMIZATION_STRATEGIES,
                        new ArrayList<>());

        strategyClassNames.forEach(
                strategyClassName -> {
                    try {
                        Class<? extends StreamGraphOptimizationStrategy> clazz =
                                Class.forName(strategyClassName)
                                        .asSubclass(StreamGraphOptimizationStrategy.class);
                        strategies.add(clazz.getDeclaredConstructor().newInstance());
                    } catch (Exception e) {
                        LOG.error(
                                "Failed to load StreamGraphOptimizationStrategy: "
                                        + strategyClassName,
                                e);
                        throw new RuntimeException(e);
                    }
                });

        return strategies;
    }

    public void optimizeStreamGraph(OperatorsFinished operatorsFinished, StreamGraphContext context)
            throws Exception {
        for (StreamGraphOptimizationStrategy strategy : strategies) {
            strategy.maybeOptimizeStreamGraph(operatorsFinished, context);
        }
    }
}
