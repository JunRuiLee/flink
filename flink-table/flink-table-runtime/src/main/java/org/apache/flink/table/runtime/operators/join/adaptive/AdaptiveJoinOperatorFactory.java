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

package org.apache.flink.table.runtime.operators.join.adaptive;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.AdaptiveJoin;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.SwitchBroadcastSide;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.generated.GeneratedProjection;
import org.apache.flink.table.runtime.operators.join.HashJoinOperator;
import org.apache.flink.table.runtime.operators.join.HashJoinType;
import org.apache.flink.table.runtime.operators.join.SortMergeJoinFunction;
import org.apache.flink.table.runtime.operators.join.SortMergeJoinOperator;
import org.apache.flink.table.runtime.planner.adapter.HashJoinCodegenAdapter;
import org.apache.flink.table.types.logical.RowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Adaptive join factory.
 *
 * @param <OUT> The output type of the operator
 */
@Internal
public class AdaptiveJoinOperatorFactory<OUT> extends AbstractStreamOperatorFactory<OUT>
        implements AdaptiveJoin {
    private static final long serialVersionUID = 1L;

    private final Logger log = LoggerFactory.getLogger(AdaptiveJoinOperatorFactory.class);

    private final List<PotentialBroadcastSide> potentialBroadcastJoinSides;

    private final HashJoinType hashJoinType;

    private final RowType keyType;

    private final RowType leftType;

    private final RowType rightType;

    private final int[] leftKeyMapping;

    private final int[] rightKeyMapping;

    private final GeneratedProjection leftProj;

    private final GeneratedProjection rightProj;

    private final int leftRowSize;

    private final long leftRowCount;

    private final int rightRowSize;

    private final long rightRowCount;

    private final GeneratedJoinCondition condFunc;

    private final boolean compressionEnabled;

    private final int compressionBlockSize;

    private final SortMergeJoinFunction sortMergeJoinFunction;

    private final int originalJobType;

    private final boolean[] filterNullKeys;

    private final boolean supportCodegen;

    private final boolean tryDistinctBuildRow;

    private boolean leftIsBuild;

    private boolean isBroadcastJoin;

    private StreamOperatorFactory<OUT> finalFactory;

    public AdaptiveJoinOperatorFactory(
            HashJoinType hashJoinType,
            RowType keyType,
            RowType leftType,
            RowType rightType,
            int[] leftKeyMapping,
            int[] rightKeyMapping,
            GeneratedProjection leftProj,
            GeneratedProjection rightProj,
            int leftRowSize,
            long leftRowCount,
            int rightRowSize,
            long rightRowCount,
            GeneratedJoinCondition condFunc,
            boolean leftIsBuild,
            boolean compressionEnabled,
            int compressionBlockSize,
            SortMergeJoinFunction sortMergeJoinFunction,
            int maybeBroadcastJoinSide,
            int originalJobType,
            boolean[] filterNullKeys,
            boolean tryDistinctBuildRow,
            boolean supportCodegen) {
        this.hashJoinType = hashJoinType;
        this.keyType = keyType;
        this.leftType = leftType;
        this.rightType = rightType;
        this.leftKeyMapping = leftKeyMapping;
        this.rightKeyMapping = rightKeyMapping;
        this.leftProj = leftProj;
        this.rightProj = rightProj;
        this.leftRowSize = leftRowSize;
        this.leftRowCount = leftRowCount;
        this.rightRowSize = rightRowSize;
        this.rightRowCount = rightRowCount;
        this.condFunc = condFunc;
        this.leftIsBuild = leftIsBuild;
        this.compressionEnabled = compressionEnabled;
        this.compressionBlockSize = compressionBlockSize;
        this.sortMergeJoinFunction = sortMergeJoinFunction;
        this.originalJobType = originalJobType;
        this.filterNullKeys = filterNullKeys;
        this.tryDistinctBuildRow = tryDistinctBuildRow;
        this.supportCodegen = supportCodegen;

        potentialBroadcastJoinSides = new ArrayList<>();
        if (maybeBroadcastJoinSide == 0) {
            potentialBroadcastJoinSides.add(PotentialBroadcastSide.LEFT);
        } else if (maybeBroadcastJoinSide == 1) {
            potentialBroadcastJoinSides.add(PotentialBroadcastSide.RIGHT);
        } else if (maybeBroadcastJoinSide == 2) {
            potentialBroadcastJoinSides.add(PotentialBroadcastSide.LEFT);
            potentialBroadcastJoinSides.add(PotentialBroadcastSide.RIGHT);
        }
    }

    @Override
    public void genOperatorFactory(ClassLoader classLoader, ReadableConfig config) {
        StreamOperatorFactory<RowData> operatorFactory = null;
        if (isBroadcastJoin || originalJobType == 0) {
            boolean reverseJoin = !leftIsBuild;
            GeneratedProjection buildProj;
            GeneratedProjection probeProj;
            RowType buildType;
            RowType probeType;
            int buildRowSize;
            long buildRowCount;
            int[] buildKeys;
            long probeRowCount;
            int[] probeKeys;

            if (leftIsBuild) {
                buildProj = leftProj;
                buildType = leftType;
                buildRowSize = leftRowSize;
                buildRowCount = leftRowCount;
                buildKeys = leftKeyMapping;

                probeProj = rightProj;
                probeType = rightType;
                probeRowCount = rightRowCount;
                probeKeys = rightKeyMapping;
            } else {
                buildProj = rightProj;
                buildType = rightType;
                buildRowSize = rightRowSize;
                buildRowCount = rightRowCount;
                buildKeys = rightKeyMapping;

                probeProj = leftProj;
                probeType = leftType;
                probeRowCount = leftRowCount;
                probeKeys = leftKeyMapping;
            }

            if (supportCodegen) {
                try {
                    HashJoinCodegenAdapter hashJoinCodegenAdapter = new HashJoinCodegenAdapter(
                            config,
                            classLoader,
                            hashJoinType,
                            keyType,
                            buildType,
                            probeType,
                            buildKeys,
                            probeKeys,
                            buildRowSize,
                            buildRowCount,
                            reverseJoin,
                            condFunc,
                            leftIsBuild,
                            compressionEnabled,
                            compressionBlockSize,
                            sortMergeJoinFunction);

                    long startTimeMs = System.currentTimeMillis();
                    operatorFactory = hashJoinCodegenAdapter.gen();
                    log.info(
                            "[POC] codegen use time {} ms.",
                            System.currentTimeMillis() - startTimeMs);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            } else {
                operatorFactory = SimpleOperatorFactory.of(
                        HashJoinOperator.newHashJoinOperator(
                                hashJoinType,
                                leftIsBuild,
                                compressionEnabled,
                                compressionBlockSize,
                                condFunc,
                                reverseJoin,
                                filterNullKeys,
                                buildProj,
                                probeProj,
                                tryDistinctBuildRow,
                                buildRowSize,
                                buildRowCount,
                                probeRowCount,
                                keyType,
                                sortMergeJoinFunction));
            }
        } else {
            operatorFactory = SimpleOperatorFactory.of(new SortMergeJoinOperator(sortMergeJoinFunction));
        }

        finalFactory = (StreamOperatorFactory<OUT>) operatorFactory;
    }

    @Override
    public int getStaticBuildSide() {
        if (leftIsBuild) {
            return 1;
        } else {
            return 2;
        }
    }

    @Override
    public void markRealBuildSide(int side) {
        isBroadcastJoin = true;
        if (side == 1) {
            leftIsBuild = true;
        } else {
            leftIsBuild = false;
        }
    }

    @Override
    public List<PotentialBroadcastSide> getPotentialBroadcastJoinSides() {
        return potentialBroadcastJoinSides;
    }

    @Override
    public <T extends StreamOperator<OUT>> T createStreamOperator(StreamOperatorParameters<OUT> parameters) {
        if (finalFactory instanceof AbstractStreamOperatorFactory) {
            ((AbstractStreamOperatorFactory) finalFactory).setProcessingTimeService(processingTimeService);
        }
        StreamOperator<OUT> operator = finalFactory.createStreamOperator(parameters);
        if (isBroadcastJoin && operator instanceof SwitchBroadcastSide) {
            ((SwitchBroadcastSide) operator)
                    .activateBroadcastJoin(leftIsBuild);
        }
        return (T) operator;
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return finalFactory.getStreamOperatorClass(classLoader);
    }
}
