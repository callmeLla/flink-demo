package com.ljn.flinksql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Created by search-lemon on 2020/10/27.
 * 读取kafka数据，解析后输出到控制台.
 */
public class StreamSqlTest {

    // use blink planner
    private static TableEnvironment tableEnv;

    public static void main(String[] args) throws Exception {
        initBlinkEnv();
        registerKafkaSource();
        print();
    }

    private static void initBlinkEnv() {
        EnvironmentSettings tableEnvSettings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        // stream也可使用StreamTableEnvironment
        tableEnv = TableEnvironment.create(tableEnvSettings);
    }

    /**
     * Flink 1.11只支持kafka 10和11版本.
     */
    private static void registerKafkaSource() {

        String sourceSql = "CREATE TABLE flink_input_test ("
                + " logtime STRING,"
                + " sign STRING,"
                + " version STRING"
                + " ) WITH ( "
                + " 'connector' = 'kafka',"
                + " 'topic' = 'flink_input_test'," // kafak topic
                + " 'properties.bootstrap.servers' = '*******'," // kafak brokers
                + " 'properties.group.id' = 'test_20201102'," // kafka group.id
                + " 'format' = 'json'," // kafka中数据格式
                + " 'scan.startup.mode' = 'latest-offset'" // 设置从最新offset开始消费
                + ")";

        tableEnv.executeSql(sourceSql);
    }

    /**
     * 注册print table并输出数据.
     */
    private static void print() {

        String printTable = "CREATE TABLE print_table"
                + " WITH ('connector' = 'print')"
                + " LIKE flink_input_test (EXCLUDING ALL)"; // 这里使用LIKE直接创建注册print表

        tableEnv.executeSql(printTable);

        String printData = "INSERT INTO print_table"
                + " SELECT logtime, sign, version"
                + " FROM flink_input_test";

        tableEnv.executeSql(printData);

    }
}
