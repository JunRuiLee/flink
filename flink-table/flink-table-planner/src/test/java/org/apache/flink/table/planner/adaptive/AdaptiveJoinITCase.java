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

package org.apache.flink.table.planner.adaptive;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.BatchExecutionOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.catalog.ConnectorCatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.plan.batch.sql.TpcdsSchema;
import org.apache.flink.table.planner.plan.batch.sql.TpcdsSchemaProvider;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

/** Plan test for dynamic filtering. */
@ExtendWith(ParameterizedTestExtension.class)
class AdaptiveJoinITCase extends TableTestBase {

    TableEnvironment env;

    @BeforeEach
    void before() {
        Configuration config = new Configuration();
        config.set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        //        config.set(ExecutionOptions.BATCH_SHUFFLE_MODE,
        // BatchShuffleMode.ALL_EXCHANGES_BLOCKING);
        config.set(BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_MAX_PARALLELISM, 8);
        config.set(BatchExecutionOptions.SPECULATIVE_ENABLED, true);

        config.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        config.set(
                OptimizerConfigOptions.TABLE_OPTIMIZER_ADAPTIVE_BROADCAST_JOIN_STRATEGY,
                OptimizerConfigOptions.AdaptiveBroadcastJoinStrategy.RUNTIME_ONLY);
        //
        // config.set(OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD,
        // 1111111111111111L);

        env = TableEnvironment.create(config);

        // prepare data
        List<Row> data1 = new ArrayList<>();
        data1.addAll(getRepeatedRow(2, 100));
        data1.addAll(getRepeatedRow(5, 100));
        data1.addAll(getRepeatedRow(10, 100));
        String dataId1 = TestValuesTableFactory.registerData(data1);

        List<Row> data2 = new ArrayList<>();
        data2.addAll(getRepeatedRow(5, 10));
        data2.addAll(getRepeatedRow(10, 10));
        data2.addAll(getRepeatedRow(20, 10));
        String dataId2 = TestValuesTableFactory.registerData(data2);

        env.executeSql(
                String.format(
                        "CREATE TABLE t1 (\n"
                                + "  x INT,\n"
                                + "  y BIGINT,\n"
                                + "  z VARCHAR\n"
                                + ")  WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'data-id' = '%s',\n"
                                + " 'bounded' = 'true'\n"
                                + ")",
                        dataId1));

        env.executeSql(
                String.format(
                        "CREATE TABLE t2 (\n"
                                + "  a INT,\n"
                                + "  b BIGINT,\n"
                                + "  c VARCHAR\n"
                                + ")  WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'data-id' = '%s',\n"
                                + " 'bounded' = 'true'\n"
                                + ")",
                        dataId2));

        env.executeSql(
                String.format(
                        "CREATE TABLE t3 (\n"
                                + "  a2 INT,\n"
                                + "  b2 BIGINT,\n"
                                + "  c2 VARCHAR\n"
                                + ")  WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'data-id' = '%s',\n"
                                + " 'bounded' = 'true'\n"
                                + ")",
                        dataId2));

        env.executeSql(
                "CREATE TABLE sink (\n"
                        + "  x INT,\n"
                        + "  z VARCHAR,\n"
                        + "  a INT,\n"
                        + "  b BIGINT,\n"
                        + "  c VARCHAR\n"
                        + ")  WITH (\n"
                        + " 'connector' = 'values',\n"
                        + " 'bounded' = 'true'\n"
                        + ")");
    }

    @AfterEach
    void after() {
        TestValuesTableFactory.clearAllData();
    }

    @Test
    void testHashJoin() throws Exception {
        env.executeSql("SELECT /*+ SHUFFLE_HASH(t1) */ x, z, a, b, c FROM t1 JOIN t2 ON t1.x=t2.a")
                .print();
    }

    @Test
    void testBroadcast() throws Exception {
        env.executeSql("SELECT /*+ BROADCAST(t2) */ x, z, a, b, c FROM t1 JOIN t2 ON t1.x=t2.a")
                .print();
    }

    @Test
    void testSortMerge() throws Exception {
        env.executeSql("SELECT /*+ SHUFFLE_MERGE(t1) */ x, z, a, b, c FROM t1 JOIN t2 ON t1.x=t2.a")
                .print();
    }

    @Test
    void testUnionJoin() throws Exception {
        env.getConfig()
                .set(
                        ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS,
                        "ShuffleHashJoin,NestedLoopJoin");
        env.executeSql(
                        "with tUnion as\n"
                                + " (select a,b,c"
                                + "  from (select a,b,c from t2 "
                                + "        union all\n"
                                + "        select a2 a, b2 b, c2 c from t3))"
                                + "SELECT  x, z, a, b, c FROM t1 JOIN tUnion ON t1.x=tUnion.a")
                .print();
    }

    private static final List<String> TPCDS_TABLES =
            Arrays.asList(
                    "catalog_sales",
                    "catalog_returns",
                    "inventory",
                    "store_sales",
                    "store_returns",
                    "web_sales",
                    "web_returns",
                    "call_center",
                    "catalog_page",
                    "customer",
                    "customer_address",
                    "customer_demographics",
                    "date_dim",
                    "household_demographics",
                    "income_band",
                    "item",
                    "promotion",
                    "reason",
                    "ship_mode",
                    "store",
                    "time_dim",
                    "warehouse",
                    "web_page",
                    "web_site");
    private static final List<String> TPCDS_QUERIES = Arrays.asList("1");
    //            Arrays.asList(
    //                    "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13",
    // "14a",
    //                    "14b", "15", "16", "17", "18", "19", "20", "21", "22", "23a", "23b",
    // "24a",
    //                    "24b", "25", "26", "27", "28", "29", "30", "31", "32", "33", "34", "35",
    // "36",
    //                    "37", "38", "39a", "39b", "40", "41", "42", "43", "44", "45", "46", "47",
    // "48",
    //                    "49", "50", "51", "52", "53", "54", "55", "56", "57", "58", "59", "60",
    // "61",
    //                    "62", "63", "64", "65", "66", "67", "68", "69", "70", "71", "72", "73",
    // "74",
    //                    "75", "76", "77", "78", "79", "80", "81", "82", "83", "84", "85", "86",
    // "87",
    //                    "88", "89", "90", "91", "92", "93", "94", "95", "96", "97", "98", "99");

    private static final String QUERY_PREFIX = "query";
    private static final String QUERY_SUFFIX = ".sql";
    private static final String DATA_SUFFIX = ".dat";
    private static final String RESULT_SUFFIX = ".ans";
    private static final String COL_DELIMITER = "|";
    private static final String FILE_SEPARATOR = "/";

    @Test
    void testTpcDs() throws Exception {
        String sourceTablePath =
                "/Users/sunxia.sx/IdeaProjects/flink-aqe/flink-end-to-end-tests/flink-tpcds-test/target/table";
        String queryPath =
                "/Users/sunxia.sx/IdeaProjects/flink-aqe/flink-end-to-end-tests/flink-tpcds-test/tpcds-tool/query";
        String sinkTablePath =
                "/Users/sunxia.sx/IdeaProjects/flink-aqe/flink-end-to-end-tests/flink-tpcds-test/target/itResult";
        Boolean useTableStats = false;
        TableEnvironment tableEnvironment = prepareTableEnv(sourceTablePath, useTableStats);

        // execute TPC-DS queries
        for (String queryId : TPCDS_QUERIES) {
            System.out.println("[INFO]Run TPC-DS query " + queryId + " ...");
            String queryName = QUERY_PREFIX + queryId + QUERY_SUFFIX;
            String queryFilePath = queryPath + FILE_SEPARATOR + queryName;
            String queryString = loadFile2String(queryFilePath);
            Table resultTable = tableEnvironment.sqlQuery(queryString);

            // register sink table
            String sinkTableName = QUERY_PREFIX + queryId + "_sinkTable";
            ((TableEnvironmentInternal) tableEnvironment)
                    .registerTableSinkInternal(
                            sinkTableName,
                            new CsvTableSink(
                                    sinkTablePath + FILE_SEPARATOR + queryId + RESULT_SUFFIX,
                                    COL_DELIMITER,
                                    1,
                                    FileSystem.WriteMode.OVERWRITE,
                                    resultTable.getSchema().getFieldNames(),
                                    resultTable.getSchema().getFieldDataTypes()));
            TableResult tableResult = resultTable.executeInsert(sinkTableName);
            // wait job finish
            tableResult.getJobClient().get().getJobExecutionResult().get();
            System.out.println("[INFO]Run TPC-DS query " + queryId + " success.");
        }
    }

    private static TableEnvironment prepareTableEnv(String sourceTablePath, Boolean useTableStats) {
        // init Table Env
        EnvironmentSettings environmentSettings = EnvironmentSettings.inBatchMode();
        TableEnvironment tEnv = TableEnvironment.create(environmentSettings);

        // config Optimizer parameters
        // TODO use the default shuffle mode of batch runtime mode once FLINK-23470 is implemented
        // config Optimizer parameters
        tEnv.getConfig()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD,
                        10 * 1024 * 1024L);
        tEnv.getConfig().set(OptimizerConfigOptions.TABLE_OPTIMIZER_JOIN_REORDER_ENABLED, true);
        tEnv.getConfig()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_ADAPTIVE_BROADCAST_JOIN_STRATEGY,
                        OptimizerConfigOptions.AdaptiveBroadcastJoinStrategy.RUNTIME_ONLY);
        tEnv.getConfig()
                .set(
                        ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS,
                        "ShuffleHashJoin,NestedLoopJoin");

        // register TPC-DS tables
        TPCDS_TABLES.forEach(
                table -> {
                    TpcdsSchema schema = TpcdsSchemaProvider.getTableSchema(table);
                    CsvTableSource.Builder builder = CsvTableSource.builder();
                    builder.path(sourceTablePath + FILE_SEPARATOR + table + DATA_SUFFIX);
                    for (int i = 0; i < schema.getFieldNames().size(); i++) {
                        builder.field(
                                schema.getFieldNames().get(i),
                                TypeConversions.fromDataTypeToLegacyInfo(
                                        schema.getFieldTypes().get(i)));
                    }
                    builder.fieldDelimiter(COL_DELIMITER);
                    builder.emptyColumnAsNull();
                    builder.lineDelimiter("\n");
                    CsvTableSource tableSource = builder.build();
                    ConnectorCatalogTable catalogTable =
                            ConnectorCatalogTable.source(tableSource, true);
                    tEnv.getCatalog(tEnv.getCurrentCatalog())
                            .ifPresent(
                                    catalog -> {
                                        try {
                                            catalog.createTable(
                                                    new ObjectPath(
                                                            tEnv.getCurrentDatabase(), table),
                                                    catalogTable,
                                                    false);
                                        } catch (Exception e) {
                                            throw new RuntimeException(e);
                                        }
                                    });
                });

        return tEnv;
    }

    private static String loadFile2String(String filePath) throws Exception {
        StringBuilder stringBuilder = new StringBuilder();
        Stream<String> stream = Files.lines(Paths.get(filePath), StandardCharsets.UTF_8);
        stream.forEach(s -> stringBuilder.append(s).append('\n'));
        return stringBuilder.toString();
    }

    private List<Row> getRepeatedRow(int key, int nums) {
        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < nums; i++) {
            rows.add(Row.of(key, (long) key, String.valueOf(key)));
        }
        return rows;
    }
}
