package org.apache.flink.table.runtime.planner.adapter;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.CodeGenOperatorFactory;


public abstract class CodegenAdapter {

    private final transient ReadableConfig readableConfig;

    private final transient ClassLoader classLoader;

    protected CodegenAdapter(ReadableConfig readableConfig, ClassLoader classLoader) {
        this.readableConfig = readableConfig;
        this.classLoader = classLoader;
    }

    CodeGenOperatorFactory<RowData> gen() {
        throw new RuntimeException("Must be implemented.");
    }

    public ReadableConfig getReadableConfig() {
        return readableConfig;
    }

    public ClassLoader getClassLoader() {
        return classLoader;
    }

}
