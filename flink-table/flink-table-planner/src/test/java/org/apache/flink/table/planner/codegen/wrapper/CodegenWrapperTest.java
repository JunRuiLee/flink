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

package org.apache.flink.table.planner.codegen.wrapper;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.planner.codegen.HashJoinCodegenWrapper;
import org.apache.flink.table.planner.codegen.LongHashJoinGenerator;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.utils.SorMergeJoinOperatorUtil;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.generated.JoinCondition;
import org.apache.flink.table.runtime.operators.CodeGenOperatorFactory;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.HashJoinType;

import org.apache.flink.table.runtime.operators.join.Int2HashJoinOperatorTestBase;
import org.apache.flink.table.runtime.operators.join.SortMergeJoinFunction;
import org.apache.flink.table.planner.adapter.HashJoinCodegenParameter;
import org.apache.flink.table.runtime.util.UniformBinaryRowGenerator;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.planner.adapter.CodegenWrapper;

import org.apache.flink.util.MutableObjectIterator;

import org.junit.jupiter.api.Test;

import java.io.Serializable;

import static org.apache.flink.table.runtime.operators.join.Int2HashJoinOperatorTestBase.joinAndAssert;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link CodegenWrapper}. */
class CodegenWrapperTest implements Serializable {

    @Test
    void testGenLongHashJoinOperator() throws Exception {
        RowType keyType = RowType.of(new IntType());
        boolean[] filterNulls = new boolean[] {true};
        HashJoinType hashJoinType = HashJoinType.INNER;
        assertThat(LongHashJoinGenerator.support(hashJoinType, keyType, filterNulls)).isTrue();

        RowType buildType = RowType.of(new IntType(), new IntType());
        RowType probeType = RowType.of(new IntType(), new IntType());
        int[] buildKeyMapping = new int[] {0};
        int[] probeKeyMapping = new int[] {0};
        GeneratedJoinCondition condFunc =
                new GeneratedJoinCondition(
                        Int2HashJoinOperatorTestBase.MyJoinCondition.class.getCanonicalName(), "", new Object[0]) {
                    @Override
                    public JoinCondition newInstance(ClassLoader classLoader) {
                        return new Int2HashJoinOperatorTestBase.MyJoinCondition(new Object[0]);
                    }
                };

        boolean buildLeft = true;
        FlinkJoinType flinkJoinType = FlinkJoinType.INNER;
        SortMergeJoinFunction sortMergeJoinFunction =
                    SorMergeJoinOperatorUtil.getSortMergeJoinFunction(
                            Thread.currentThread().getContextClassLoader(),
                            ExecNodeConfig.ofNodeConfig(new Configuration(), false),
                            flinkJoinType,
                            buildType,
                            probeType,
                            buildKeyMapping,
                            probeKeyMapping,
                            keyType,
                            buildLeft,
                            filterNulls,
                            condFunc,
                            0);
        HashJoinCodegenParameter parameter = new HashJoinCodegenParameter(
                new Configuration(),
                Thread.currentThread().getContextClassLoader(),
                hashJoinType,
                keyType,
                buildType,
                probeType,
                buildKeyMapping,
                probeKeyMapping,
                20,
                10000,
                !buildLeft,
                condFunc,
                buildLeft,
                ExecutionConfigOptions.TABLE_EXEC_SPILL_COMPRESSION_ENABLED.defaultValue(),
                (int)
                        ExecutionConfigOptions.TABLE_EXEC_SPILL_COMPRESSION_BLOCK_SIZE
                                .defaultValue()
                                .getBytes(),
                sortMergeJoinFunction);

        HashJoinCodegenWrapper wrapper = new HashJoinCodegenWrapper();
        CodeGenOperatorFactory<RowData> operator = wrapper.codegen(parameter);

        int numKeys = 10;
        int buildValsPerKey = 3;
        int probeValsPerKey = 10;
        MutableObjectIterator<BinaryRowData> buildInput =
                new UniformBinaryRowGenerator(numKeys, buildValsPerKey, false);
        MutableObjectIterator<BinaryRowData> probeInput =
                new UniformBinaryRowGenerator(numKeys, probeValsPerKey, true);

        joinAndAssert(operator, buildInput, probeInput, 300, 10, 165, false);
    }
}
