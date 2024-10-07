/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.adapter;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.loader.PlannerModule;
import org.apache.flink.table.runtime.operators.CodeGenOperatorFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

/** An adapter for accessing code gen method in flink-table-planner module. */
public class CodegenAdapter {

    /**
     * This method locates a unique code generation class based on the provided identifier.
     * It then passes the parameter to the code gen method to create an instance of
     * {@link CodeGenOperatorFactory).
     *
     * <p> This method uses {@link PlannerModule} to access the planner code, so it will also be
     * subject to relevant restrictions, such as the flink-table-planner jar should being accessible.
     */
    public static CodeGenOperatorFactory<RowData> codegenOperatorFactory(CodegenIdentifier identifier, CodegenAdapter.CodegenParameter parameter) {
        return discoverCodegenWrapper(identifier, parameter.getClassLoader())
                .codegen(parameter);
    }

    static CodegenWrapper discoverCodegenWrapper(CodegenIdentifier identifier, ClassLoader classLoader) {
        List<CodegenWrapper> matchedCodegenAdapters = tryDiscover(
                        CodegenWrapper.class,  classLoader)
                .orElseGet(() -> PlannerModule.getInstance().discover(CodegenWrapper.class))
                .stream()
                .filter(wrapper -> wrapper.match(identifier)).collect(Collectors.toList());
        if (matchedCodegenAdapters.size() != 1) {
            throw new RuntimeException(
                    "Found "
                            + matchedCodegenAdapters.size()
                            + " classes implementing "
                            + CodegenWrapper.class.getName()
                            + ". They are:\n"
                            + matchedCodegenAdapters.stream()
                            .map(t -> t.getClass().getName())
                            .collect(Collectors.joining("\n")));
        }

        return matchedCodegenAdapters.get(0);
    }

    public static <T> Optional<List<T>> tryDiscover(Class<T> clazz, ClassLoader classLoader) {
        List<T> results = new ArrayList<>();
        ServiceLoader.load(clazz, classLoader).iterator().forEachRemaining(results::add);
        if (results.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(results);
    }

    /**
     * This class is used to provide the parameter required for code generation.
     * All classes implementing the CodegenWrapper interface must use this class as a base class
     * for the necessary parameters.
     */
    public static class CodegenParameter implements Serializable {

        private final transient ReadableConfig readableConfig;

        private final transient ClassLoader classLoader;

        public CodegenParameter(
                ReadableConfig readableConfig,
                ClassLoader classLoader) {
            this.readableConfig = readableConfig;
            this.classLoader = classLoader;
        }

        public ReadableConfig getReadableConfig() {
            return readableConfig;
        }

        public ClassLoader getClassLoader() {
            return classLoader;
        }
    }

    /** Identifier for a unique code gen class. */
    public enum CodegenIdentifier {
        HASH_JOIN
    }
}
