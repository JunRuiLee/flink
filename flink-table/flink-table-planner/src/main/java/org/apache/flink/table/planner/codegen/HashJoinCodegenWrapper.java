package org.apache.flink.table.planner.codegen;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.CodeGenOperatorFactory;
import org.apache.flink.table.planner.adapter.CodegenAdapter.CodegenIdentifier;
import org.apache.flink.table.planner.adapter.CodegenWrapper;
import org.apache.flink.table.planner.adapter.HashJoinCodegenParameter;

/** A codegen wrapper for  {@link LongHashJoinGenerator}. */
public class HashJoinCodegenWrapper implements CodegenWrapper<HashJoinCodegenParameter> {

    @Override
    public CodeGenOperatorFactory<RowData> codegen(HashJoinCodegenParameter parameter) {
        return LongHashJoinGenerator.gen(parameter.getReadableConfig(),
                parameter.getClassLoader(),
                parameter.getHashJoinType(),
                parameter.getKeyType(),
                parameter.getBuildType(),
                parameter.getProbeType(),
                parameter.getBuildKeys(),
                parameter.getProbeKeys(),
                parameter.getBuildRowSize(),
                parameter.getBuildRowCount(),
                parameter.isReverseJoinFunction(),
                parameter.getCondFunc(),
                parameter.isLeftIsBuild(),
                parameter.isCompressionEnabled(),
                parameter.getCompressionBlockSize(),
                parameter.getSortMergeJoinFunction());
    }

    @Override
    public CodegenIdentifier getIdentifier() {
        return CodegenIdentifier.HASH_JOIN;
    }
}
