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

package org.apache.flink.table.planner.plan.batch.sql;

import org.apache.flink.api.common.BatchShuffleMode;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.BatchExecutionOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.List;

/** Plan test for dynamic filtering. */
@ExtendWith(ParameterizedTestExtension.class)
class AdaptiveJoinITCase extends TableTestBase {

    TableEnvironment env;

    @BeforeEach
    void before() {
        Configuration config = new Configuration();
        config.set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 10);
        config.set(ExecutionOptions.BATCH_SHUFFLE_MODE, BatchShuffleMode.ALL_EXCHANGES_BLOCKING);
        config.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        config.set(OptimizerConfigOptions.TABLE_OPTIMIZER_ADAPTIVE_JOIN_ENABLED, true);
        config.set(DeploymentOptions.SUBMIT_STREAM_GRAPH_ENABLED, true);
        config.set(BatchExecutionOptions.ADAPTIVE_JOIN_TYPE_ENABLED, true);

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
    void testQ2() {
        List<Row> data1 = new ArrayList<>();
        data1.add(Row.of("123", 123));
        env.executeSql(
                String.format(
                        "CREATE TABLE web_sales (\n"
                                + "  ws_sold_date_sk VARCHAR,\n"
                                + "  ws_ext_sales_price INT\n"
                                + ")  WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'data-id' = '%s',\n"
                                + " 'bounded' = 'true'\n"
                                + ")",
                        TestValuesTableFactory.registerData(data1)));

        env.executeSql(
                String.format(
                        "CREATE TABLE catalog_sales (\n"
                                + "  cs_sold_date_sk VARCHAR,\n"
                                + "  cs_ext_sales_price INT\n"
                                + ")  WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'data-id' = '%s',\n"
                                + " 'bounded' = 'true'\n"
                                + ")",
                        TestValuesTableFactory.registerData(data1)));

        List<Row> data2 = new ArrayList<>();
        data2.add(Row.of("123", "Sunday", 123, 2001));
        env.executeSql(
                String.format(
                        "CREATE TABLE date_dim (\n"
                                + "  d_date_sk VARCHAR,\n"
                                + "  d_day_name VARCHAR,\n"
                                + "  d_week_seq int,\n"
                                + "  d_year int\n"
                                + ")  WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'data-id' = '%s',\n"
                                + " 'bounded' = 'true'\n"
                                + ")",
                        TestValuesTableFactory.registerData(data2)));

        env.executeSql("with wscs as\n"
                + " (select sold_date_sk\n"
                + "        ,sales_price\n"
                + "  from (select ws_sold_date_sk sold_date_sk\n"
                + "              ,ws_ext_sales_price sales_price\n"
                + "        from web_sales) x\n"
                + "        union all\n"
                + "       (select cs_sold_date_sk sold_date_sk\n"
                + "              ,cs_ext_sales_price sales_price\n"
                + "        from catalog_sales)),\n"
                + " wswscs as \n"
                + " (select d_week_seq,\n"
                + "        sum(case when (d_day_name='Sunday') then sales_price else null end) sun_sales,\n"
                + "        sum(case when (d_day_name='Monday') then sales_price else null end) mon_sales,\n"
                + "        sum(case when (d_day_name='Tuesday') then sales_price else  null end) tue_sales,\n"
                + "        sum(case when (d_day_name='Wednesday') then sales_price else null end) wed_sales,\n"
                + "        sum(case when (d_day_name='Thursday') then sales_price else null end) thu_sales,\n"
                + "        sum(case when (d_day_name='Friday') then sales_price else null end) fri_sales,\n"
                + "        sum(case when (d_day_name='Saturday') then sales_price else null end) sat_sales\n"
                + " from wscs\n"
                + "     ,date_dim\n"
                + " where d_date_sk = sold_date_sk\n"
                + " group by d_week_seq)\n"
                + " select d_week_seq1\n"
                + "       ,round(sun_sales1/sun_sales2,2)\n"
                + "       ,round(mon_sales1/mon_sales2,2)\n"
                + "       ,round(tue_sales1/tue_sales2,2)\n"
                + "       ,round(wed_sales1/wed_sales2,2)\n"
                + "       ,round(thu_sales1/thu_sales2,2)\n"
                + "       ,round(fri_sales1/fri_sales2,2)\n"
                + "       ,round(sat_sales1/sat_sales2,2)\n"
                + " from\n"
                + " (select wswscs.d_week_seq d_week_seq1\n"
                + "        ,sun_sales sun_sales1\n"
                + "        ,mon_sales mon_sales1\n"
                + "        ,tue_sales tue_sales1\n"
                + "        ,wed_sales wed_sales1\n"
                + "        ,thu_sales thu_sales1\n"
                + "        ,fri_sales fri_sales1\n"
                + "        ,sat_sales sat_sales1\n"
                + "  from wswscs,date_dim \n"
                + "  where date_dim.d_week_seq = wswscs.d_week_seq and\n"
                + "        d_year = 2001) y,\n"
                + " (select wswscs.d_week_seq d_week_seq2\n"
                + "        ,sun_sales sun_sales2\n"
                + "        ,mon_sales mon_sales2\n"
                + "        ,tue_sales tue_sales2\n"
                + "        ,wed_sales wed_sales2\n"
                + "        ,thu_sales thu_sales2\n"
                + "        ,fri_sales fri_sales2\n"
                + "        ,sat_sales sat_sales2\n"
                + "  from wswscs\n"
                + "      ,date_dim \n"
                + "  where date_dim.d_week_seq = wswscs.d_week_seq and\n"
                + "        d_year = 2001+1) z\n"
                + " where d_week_seq1=d_week_seq2-53\n"
                + " order by d_week_seq1").print();
    }

    @Test
    void testQ3() {
        List<Row> data1 = new ArrayList<>();
        data1.add(Row.of(123, 123,123, 436));
        env.executeSql(
                String.format(
                        "CREATE TABLE item (\n"
                                + "  i_brand_id int,\n"
                                + "  i_brand INT,\n"
                                + "  i_item_sk INT,\n"
                                + "  i_manufact_id INT\n"
                                + ")  WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'data-id' = '%s',\n"
                                + " 'bounded' = 'true'\n"
                                + ")",
                        TestValuesTableFactory.registerData(data1)));

        env.executeSql(
                String.format(
                        "CREATE TABLE store_sales (\n"
                                + "  ss_sold_date_sk int,\n"
                                + "  ss_ext_sales_price INT,\n"
                                + "  ss_item_sk INT,\n"
                                + "  i_item_sk INT\n"
                                + ")  WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'data-id' = '%s',\n"
                                + " 'bounded' = 'true'\n"
                                + ")",
                        TestValuesTableFactory.registerData(data1)));

        List<Row> data2 = new ArrayList<>();
        data2.add(Row.of(123, "Sunday", 12, 2001));
        env.executeSql(
                String.format(
                        "CREATE TABLE date_dim (\n"
                                + "  d_date_sk int,\n"
                                + "  d_day_name VARCHAR,\n"
                                + "  d_moy int,\n"
                                + "  d_year int\n"
                                + ")  WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'data-id' = '%s',\n"
                                + " 'bounded' = 'true'\n"
                                + ")",
                        TestValuesTableFactory.registerData(data2)));

        env.executeSql("-- start query 1 in stream 0 using template query3.tpl and seed 2031708268\n"
                + "select  dt.d_year \n"
                + "       ,item.i_brand_id brand_id \n"
                + "       ,item.i_brand brand\n"
                + "       ,sum(ss_ext_sales_price) sum_agg\n"
                + " from  date_dim dt \n"
                + "      ,store_sales\n"
                + "      ,item\n"
                + " where dt.d_date_sk = store_sales.ss_sold_date_sk\n"
                + "   and store_sales.ss_item_sk = item.i_item_sk\n"
                + "   and item.i_manufact_id = 436\n"
                + "   and dt.d_moy=12\n"
                + " group by dt.d_year\n"
                + "      ,item.i_brand\n"
                + "      ,item.i_brand_id\n"
                + " order by dt.d_year\n"
                + "         ,sum_agg desc\n"
                + "         ,brand_id\n"
                + " limit 100\n"
                + "\n"
                + "-- end query 1 in stream 0 using template query3.tpl\n").print();
    }

    @Test
    void testQ4() {
        List<Row> data1 = new ArrayList<>();
        data1.add(Row.of(123, 123,"123", "436", 1, "china", "log", 1));
        env.executeSql(
                String.format(
                        "CREATE TABLE customer (\n"
                                + "  c_customer_sk int,\n"
                                + "  c_customer_id int,\n"
                                + "  c_first_name VARCHAR,\n"
                                + "  c_last_name VARCHAR,\n"
                                + "  c_preferred_cust_flag INT,\n"
                                + "  c_birth_country VARCHAR,\n"
                                + "  c_login VARCHAR,\n"
                                + "  c_email_address INT\n"
                                + ")  WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'data-id' = '%s',\n"
                                + " 'bounded' = 'true'\n"
                                + ")",
                        TestValuesTableFactory.registerData(data1)));

        List<Row> data2 = new ArrayList<>();
        data2.add(Row.of(123, 123,123, 436,123, 436));
        env.executeSql(
                String.format(
                        "CREATE TABLE store_sales (\n"
                                + "  ss_sold_date_sk int,\n"
                                + "  ss_customer_sk int,\n"
                                + "  ss_ext_list_price int,\n"
                                + "  ss_ext_wholesale_cost INT,\n"
                                + "  ss_ext_discount_amt INT,\n"
                                + "  ss_ext_sales_price INT\n"
                                + ")  WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'data-id' = '%s',\n"
                                + " 'bounded' = 'true'\n"
                                + ")",
                        TestValuesTableFactory.registerData(data2)));

        List<Row> data4 = new ArrayList<>();
        data4.add(Row.of(123, 123,123, 436,123, 436));
        env.executeSql(
                String.format(
                        "CREATE TABLE catalog_sales (\n"
                                + "  cs_sold_date_sk int,\n"
                                + "  cs_bill_customer_sk int,\n"
                                + "  cs_ext_list_price int,\n"
                                + "  cs_ext_wholesale_cost INT,\n"
                                + "  cs_ext_discount_amt INT,\n"
                                + "  cs_ext_sales_price INT\n"
                                + ")  WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'data-id' = '%s',\n"
                                + " 'bounded' = 'true'\n"
                                + ")",
                        TestValuesTableFactory.registerData(data4)));

        List<Row> data5 = new ArrayList<>();
        data5.add(Row.of(123, 123,123, 436,123, 436));
        env.executeSql(
                String.format(
                        "CREATE TABLE web_sales (\n"
                                + "  ws_sold_date_sk int,\n"
                                + "  ws_bill_customer_sk int,\n"
                                + "  ws_ext_list_price int,\n"
                                + "  ws_ext_wholesale_cost INT,\n"
                                + "  ws_ext_discount_amt INT,\n"
                                + "  ws_ext_sales_price INT\n"
                                + ")  WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'data-id' = '%s',\n"
                                + " 'bounded' = 'true'\n"
                                + ")",
                        TestValuesTableFactory.registerData(data5)));


        List<Row> data3 = new ArrayList<>();
        data3.add(Row.of(123, "Sunday", 12, 2001));
        env.executeSql(
                String.format(
                        "CREATE TABLE date_dim (\n"
                                + "  d_date_sk int,\n"
                                + "  d_day_name VARCHAR,\n"
                                + "  d_moy int,\n"
                                + "  d_year int\n"
                                + ")  WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'data-id' = '%s',\n"
                                + " 'bounded' = 'true'\n"
                                + ")",
                        TestValuesTableFactory.registerData(data3)));

        List<Row> data6 = new ArrayList<>();
        data6.add(Row.of(123, "Sunday", 12, 2001));
        env.executeSql(
                String.format(
                        "CREATE TABLE year_total (\n"
                                + "  customer_id int,\n"
                                + "  sale_type VARCHAR,\n"
                                + "  year_total int,\n"
                                + "  d_year int\n"
                                + ")  WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'data-id' = '%s',\n"
                                + " 'bounded' = 'true'\n"
                                + ")",
                        TestValuesTableFactory.registerData(data6)));

        env.executeSql("-- start query 1 in stream 0 using template query4.tpl and seed 1819994127\n"
                + "with year_total as (\n"
                + " select c_customer_id customer_id\n"
                + "       ,c_first_name customer_first_name\n"
                + "       ,c_last_name customer_last_name\n"
                + "       ,c_preferred_cust_flag customer_preferred_cust_flag\n"
                + "       ,c_birth_country customer_birth_country\n"
                + "       ,c_login customer_login\n"
                + "       ,c_email_address customer_email_address\n"
                + "       ,d_year dyear\n"
                + "       ,sum(((ss_ext_list_price-ss_ext_wholesale_cost-ss_ext_discount_amt)+ss_ext_sales_price)/2) year_total\n"
                + "       ,'s' sale_type\n"
                + " from customer\n"
                + "     ,store_sales\n"
                + "     ,date_dim\n"
                + " where c_customer_sk = ss_customer_sk\n"
                + "   and ss_sold_date_sk = d_date_sk\n"
                + " group by c_customer_id\n"
                + "         ,c_first_name\n"
                + "         ,c_last_name\n"
                + "         ,c_preferred_cust_flag\n"
                + "         ,c_birth_country\n"
                + "         ,c_login\n"
                + "         ,c_email_address\n"
                + "         ,d_year\n"
                + " union all\n"
                + " select c_customer_id customer_id\n"
                + "       ,c_first_name customer_first_name\n"
                + "       ,c_last_name customer_last_name\n"
                + "       ,c_preferred_cust_flag customer_preferred_cust_flag\n"
                + "       ,c_birth_country customer_birth_country\n"
                + "       ,c_login customer_login\n"
                + "       ,c_email_address customer_email_address\n"
                + "       ,d_year dyear\n"
                + "       ,sum((((cs_ext_list_price-cs_ext_wholesale_cost-cs_ext_discount_amt)+cs_ext_sales_price)/2) ) year_total\n"
                + "       ,'c' sale_type\n"
                + " from customer\n"
                + "     ,catalog_sales\n"
                + "     ,date_dim\n"
                + " where c_customer_sk = cs_bill_customer_sk\n"
                + "   and cs_sold_date_sk = d_date_sk\n"
                + " group by c_customer_id\n"
                + "         ,c_first_name\n"
                + "         ,c_last_name\n"
                + "         ,c_preferred_cust_flag\n"
                + "         ,c_birth_country\n"
                + "         ,c_login\n"
                + "         ,c_email_address\n"
                + "         ,d_year\n"
                + "union all\n"
                + " select c_customer_id customer_id\n"
                + "       ,c_first_name customer_first_name\n"
                + "       ,c_last_name customer_last_name\n"
                + "       ,c_preferred_cust_flag customer_preferred_cust_flag\n"
                + "       ,c_birth_country customer_birth_country\n"
                + "       ,c_login customer_login\n"
                + "       ,c_email_address customer_email_address\n"
                + "       ,d_year dyear\n"
                + "       ,sum((((ws_ext_list_price-ws_ext_wholesale_cost-ws_ext_discount_amt)+ws_ext_sales_price)/2) ) year_total\n"
                + "       ,'w' sale_type\n"
                + " from customer\n"
                + "     ,web_sales\n"
                + "     ,date_dim\n"
                + " where c_customer_sk = ws_bill_customer_sk\n"
                + "   and ws_sold_date_sk = d_date_sk\n"
                + " group by c_customer_id\n"
                + "         ,c_first_name\n"
                + "         ,c_last_name\n"
                + "         ,c_preferred_cust_flag\n"
                + "         ,c_birth_country\n"
                + "         ,c_login\n"
                + "         ,c_email_address\n"
                + "         ,d_year\n"
                + "         )\n"
                + "  select  t_s_secyear.customer_preferred_cust_flag\n"
                + " from year_total t_s_firstyear\n"
                + "     ,year_total t_s_secyear\n"
                + "     ,year_total t_c_firstyear\n"
                + "     ,year_total t_c_secyear\n"
                + "     ,year_total t_w_firstyear\n"
                + "     ,year_total t_w_secyear\n"
                + " where t_s_secyear.customer_id = t_s_firstyear.customer_id\n"
                + "   and t_s_firstyear.customer_id = t_c_secyear.customer_id\n"
                + "   and t_s_firstyear.customer_id = t_c_firstyear.customer_id\n"
                + "   and t_s_firstyear.customer_id = t_w_firstyear.customer_id\n"
                + "   and t_s_firstyear.customer_id = t_w_secyear.customer_id\n"
                + "   and t_s_firstyear.sale_type = 's'\n"
                + "   and t_c_firstyear.sale_type = 'c'\n"
                + "   and t_w_firstyear.sale_type = 'w'\n"
                + "   and t_s_secyear.sale_type = 's'\n"
                + "   and t_c_secyear.sale_type = 'c'\n"
                + "   and t_w_secyear.sale_type = 'w'\n"
                + "   and t_s_firstyear.dyear =  2001\n"
                + "   and t_s_secyear.dyear = 2001+1\n"
                + "   and t_c_firstyear.dyear =  2001\n"
                + "   and t_c_secyear.dyear =  2001+1\n"
                + "   and t_w_firstyear.dyear = 2001\n"
                + "   and t_w_secyear.dyear = 2001+1\n"
                + "   and t_s_firstyear.year_total > 0\n"
                + "   and t_c_firstyear.year_total > 0\n"
                + "   and t_w_firstyear.year_total > 0\n"
                + "   and case when t_c_firstyear.year_total > 0 then t_c_secyear.year_total / t_c_firstyear.year_total else null end\n"
                + "           > case when t_s_firstyear.year_total > 0 then t_s_secyear.year_total / t_s_firstyear.year_total else null end\n"
                + "   and case when t_c_firstyear.year_total > 0 then t_c_secyear.year_total / t_c_firstyear.year_total else null end\n"
                + "           > case when t_w_firstyear.year_total > 0 then t_w_secyear.year_total / t_w_firstyear.year_total else null end\n"
                + " order by t_s_secyear.customer_preferred_cust_flag\n"
                + "limit 100\n"
                + "\n"
                + "-- end query 1 in stream 0 using template query4.tpl\n").print();
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

    private List<Row> getRepeatedRow(int key, int nums) {
        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < nums; i++) {
            rows.add(Row.of(key, (long) key, String.valueOf(key)));
        }
        return rows;
    }
}
