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

package org.apache.flink.table.planner.plan.nodes.exec.batch;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.operators.AdaptiveBroadcastJoin;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.adaptive.AdaptiveBroadcastJoinOperatorGenerator;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.ProjectionCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.JoinUtil;
import org.apache.flink.table.planner.plan.utils.SorMergeJoinOperatorUtil;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.SortMergeJoinFunction;
import org.apache.flink.table.runtime.operators.join.adaptive.AdaptiveBroadcastJoinOperatorFactory;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.calcite.rex.RexNode;

import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.IntStream;

/** {@link BatchExecNode} for Adaptive Broadcast Join. */
public class BatchExecAdaptiveBroadcastJoin extends ExecNodeBase<RowData>
        implements BatchExecNode<RowData>, SingleTransformationTranslator<RowData> {

    private final JoinSpec joinSpec;
    private final boolean leftIsBuild;
    private final int estimatedLeftAvgRowSize;
    private final int estimatedRightAvgRowSize;
    private final long estimatedLeftRowCount;
    private final long estimatedRightRowCount;
    private final boolean tryDistinctBuildRow;
    private final RexNode condition;
    private final String description;
    private final String originalJoinSimplifiedName;

    public BatchExecAdaptiveBroadcastJoin(
            ReadableConfig tableConfig,
            JoinSpec joinSpec,
            int estimatedLeftAvgRowSize,
            int estimatedRightAvgRowSize,
            long estimatedLeftRowCount,
            long estimatedRightRowCount,
            boolean leftIsBuild,
            boolean tryDistinctBuildRow,
            InputProperty leftInputProperty,
            InputProperty rightInputProperty,
            RowType outputType,
            String description,
            RexNode condition,
            String originalJoinSimplifiedName) {
        super(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(BatchExecAdaptiveBroadcastJoin.class),
                ExecNodeContext.newPersistedConfig(BatchExecAdaptiveBroadcastJoin.class, tableConfig),
                Arrays.asList(leftInputProperty, rightInputProperty),
                outputType,
                description);
        this.joinSpec = joinSpec;
        this.leftIsBuild = leftIsBuild;
        this.estimatedLeftAvgRowSize = estimatedLeftAvgRowSize;
        this.estimatedRightAvgRowSize = estimatedRightAvgRowSize;
        this.estimatedLeftRowCount = estimatedLeftRowCount;
        this.estimatedRightRowCount = estimatedRightRowCount;
        this.tryDistinctBuildRow = tryDistinctBuildRow;
        this.condition = condition;
        this.description = description;
        this.originalJoinSimplifiedName = originalJoinSimplifiedName;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        ExecEdge leftInputEdge = getInputEdges().get(0);
        ExecEdge rightInputEdge = getInputEdges().get(1);

        Transformation<RowData> leftInputTransform =
                (Transformation<RowData>) leftInputEdge.translateToPlan(planner);
        Transformation<RowData> rightInputTransform =
                (Transformation<RowData>) rightInputEdge.translateToPlan(planner);
        // get input types
        RowType leftType = (RowType) leftInputEdge.getOutputType();
        RowType rightType = (RowType) rightInputEdge.getOutputType();

        int[] leftKeys = joinSpec.getLeftKeys();
        int[] rightKeys = joinSpec.getRightKeys();
        LogicalType[] keyFieldTypes =
                IntStream.of(leftKeys).mapToObj(leftType::getTypeAt).toArray(LogicalType[]::new);
        RowType keyType = RowType.of(keyFieldTypes);

        GeneratedJoinCondition condFunc =
                JoinUtil.generateConditionFunction(
                        config,
                        planner.getFlinkContext().getClassLoader(),
                        condition,
                        leftType,
                        rightType);

        // operator
        FlinkJoinType joinType = joinSpec.getJoinType();

        long externalBufferMemory =
                config.get(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_EXTERNAL_BUFFER_MEMORY)
                        .getBytes();
        long managedMemory = JoinUtil.getLargeManagedMemory(joinType, config);

        // sort merge join function
        SortMergeJoinFunction sortMergeJoinFunction =
                SorMergeJoinOperatorUtil.getSortMergeJoinFunction(
                        planner.getFlinkContext().getClassLoader(),
                        config,
                        joinType,
                        leftType,
                        rightType,
                        leftKeys,
                        rightKeys,
                        keyType,
                        leftIsBuild,
                        joinSpec.getFilterNulls(),
                        condFunc,
                        1.0 * externalBufferMemory / managedMemory);

        boolean compressionEnabled =
                config.get(ExecutionConfigOptions.TABLE_EXEC_SPILL_COMPRESSION_ENABLED);
        int compressionBlockSize =
                (int)
                        config.get(ExecutionConfigOptions.TABLE_EXEC_SPILL_COMPRESSION_BLOCK_SIZE)
                                .getBytes();
        AdaptiveBroadcastJoin adaptiveBroadcastJoin = new AdaptiveBroadcastJoinOperatorGenerator(
                keyType,
                leftType,
                rightType,
                leftKeys,
                rightKeys,
                ProjectionCodeGenerator.generateProjection(
                        new CodeGeneratorContext(
                                config, planner.getFlinkContext().getClassLoader()),
                        "HashJoinLeftProjection",
                        leftType,
                        keyType,
                        leftKeys),
                ProjectionCodeGenerator.generateProjection(
                        new CodeGeneratorContext(
                                config, planner.getFlinkContext().getClassLoader()),
                        "HashJoinRightProjection",
                        rightType,
                        keyType,
                        rightKeys),
                estimatedLeftAvgRowSize,
                estimatedLeftRowCount,
                estimatedRightAvgRowSize,
                estimatedRightRowCount,
                condFunc,
                leftIsBuild,
                compressionEnabled,
                compressionBlockSize,
                sortMergeJoinFunction,
                joinType,
                originalJoinSimplifiedName,
                joinSpec.getFilterNulls(),
                tryDistinctBuildRow);


        return ExecNodeUtil.createTwoInputTransformation(
                leftInputTransform,
                rightInputTransform,
                createTransformationName(config),
                createTransformationDescription(config),
                getAdaptiveBroadcastJoinOperatorFactory(adaptiveBroadcastJoin),
                InternalTypeInfo.of(getOutputType()),
                rightInputTransform.getParallelism(),
                managedMemory,
                false);
    }

    private StreamOperatorFactory<RowData> getAdaptiveBroadcastJoinOperatorFactory(AdaptiveBroadcastJoin adaptiveBroadcastJoin) {
        try {
            byte[] adaptiveJoinSerialized = InstantiationUtil.serializeObject(adaptiveBroadcastJoin);
            return new AdaptiveBroadcastJoinOperatorFactory<>(adaptiveJoinSerialized);
        } catch (IOException e) {
            throw new TableException(
                    "Adaptive broadcast join operator serialized failed.", e);
        }
    }

    @Override
    public String getDescription() {
        return "AdaptiveBroadcastJoin("
                + "originalJoin=[" + originalJoinSimplifiedName + "], "
                + description.substring(originalJoinSimplifiedName.length() + 1);
    }
}
