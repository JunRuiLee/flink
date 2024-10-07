package org.apache.flink.table.planner.adapter;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.operators.join.HashJoinType;
import org.apache.flink.table.runtime.operators.join.SortMergeJoinFunction;
import org.apache.flink.table.types.logical.RowType;

import java.io.Serializable;

public class HashJoinCodegenParameter extends CodegenAdapter.CodegenParameter implements Serializable {
    private final HashJoinType hashJoinType;

    private final RowType keyType;

    private final RowType buildType;

    private final RowType probeType;

    private final int[] buildKeys;

    private final int[] probeKeys;

    private final GeneratedJoinCondition condFunc;

    private final boolean leftIsBuild;

    private final boolean compressionEnabled;

    private final int compressionBlockSize;

    private final SortMergeJoinFunction sortMergeJoinFunction;

    private final int buildRowSize;

    private final long buildRowCount;

    private final boolean reverseJoinFunction;

    public HashJoinCodegenParameter(
            ReadableConfig readableConfig,
            ClassLoader classLoader,
            HashJoinType hashJoinType,
            RowType keyType,
            RowType buildType,
            RowType probeType,
            int[] buildKeys,
            int[] probeKeys,
            int buildRowSize,
            long buildRowCount,
            boolean reverseJoinFunction,
            GeneratedJoinCondition condFunc,
            boolean leftIsBuild,
            boolean compressionEnabled,
            int compressionBlockSize,
            SortMergeJoinFunction sortMergeJoinFunction) {
        super(readableConfig, classLoader);
        this.hashJoinType = hashJoinType;
        this.keyType = keyType;
        this.buildType = buildType;
        this.probeType = probeType;
        this.buildKeys = buildKeys;
        this.probeKeys = probeKeys;
        this.buildRowSize = buildRowSize;
        this.buildRowCount = buildRowCount;
        this.reverseJoinFunction = reverseJoinFunction;
        this.condFunc = condFunc;
        this.leftIsBuild = leftIsBuild;
        this.compressionEnabled = compressionEnabled;
        this.compressionBlockSize = compressionBlockSize;
        this.sortMergeJoinFunction = sortMergeJoinFunction;
    }

    public int getBuildRowSize() {
        return buildRowSize;
    }

    public HashJoinType getHashJoinType() {
        return hashJoinType;
    }

    public RowType getKeyType() {
        return keyType;
    }

    public RowType getBuildType() {
        return buildType;
    }

    public RowType getProbeType() {
        return probeType;
    }

    public int[] getBuildKeys() {
        return buildKeys;
    }

    public int[] getProbeKeys() {
        return probeKeys;
    }

    public GeneratedJoinCondition getCondFunc() {
        return condFunc;
    }

    public boolean isLeftIsBuild() {
        return leftIsBuild;
    }

    public boolean isCompressionEnabled() {
        return compressionEnabled;
    }

    public int getCompressionBlockSize() {
        return compressionBlockSize;
    }

    public SortMergeJoinFunction getSortMergeJoinFunction() {
        return sortMergeJoinFunction;
    }

    public long getBuildRowCount() {
        return buildRowCount;
    }

    public boolean isReverseJoinFunction() {
        return reverseJoinFunction;
    }
}
