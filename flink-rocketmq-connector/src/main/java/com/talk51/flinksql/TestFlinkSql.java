package com.talk51.flinksql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * @ClassName: TestFlinkSql
 * @Description: 测试FlinkSQL-connector-rocketmq
 * @Author: liujianing
 * @Date: 2021/4/7 14:59
 * @Version: v1.0 文件初始创建
 */
public class TestFlinkSql {

    /** 上下文执行环境 */
    private static TableEnvironment tableEnv;


    public static void main(final String[] args) throws Exception {
        initBlinkEnv();
        registerSource();
        print();
    }

    /**
     * @Description: 加载BlinkEnv
     * @Date: 2021/4/7 15:00
     * @Author: liujianing
     * @Return
     * @Throws
     */
    private static void initBlinkEnv() {
        EnvironmentSettings tableEnvSettings = EnvironmentSettings
                .newInstance()
                /** 设置使用BlinkPlanner */
                .useBlinkPlanner()
                /** 设置流处理模式 */
                .inStreamingMode()
                .build();

        tableEnv = TableEnvironment.create(tableEnvSettings);
    }

    /**
     * @Description: 获取数据source
     * @Date: 2021/4/7 15:01
     * @Author: liujianing
     * @Return
     * @Throws
     */
    private static void registerSource() {

        String sourceSql = "CREATE TABLE url_parse_100 ("
                + " logtime STRING,"
                + " sign STRING,"
                + " version STRING"
                + " ) WITH ( "
                + " 'connector' = 'rocketmq',"
                + " 'format' = 'json',"
                + " 'nameserver' = '172.16.16.118:9876',"
                + " 'topic' = 'PushTopic',"
                + " 'group' = 'ddd'"
                + ")";

        /** 注册source表到env中 */
        tableEnv.executeSql(sourceSql);
    }

    /**
     *
     */
    /**
     * @Description: 注册print_table并输出数据.
     * @Date: 2021/4/7 15:01
     * @Author: liujianing
     * @Return
     * @Throws
     */
    private static void print() {

        String printTable = "CREATE TABLE print_table"
                + " WITH ('connector' = 'rocketmq',"
                +"'format' = 'json',"
                + " 'nameserver' = '172.16.16.119:9876',"
                + " 'topic' = 'ljntest',"
                + " 'group' = 'ddd'"
                + ")"
                + " LIKE url_parse_100 (EXCLUDING ALL)";
        /** 注册print表到env中 */
        tableEnv.executeSql(printTable);

        String printData = "INSERT INTO print_table"
                + " SELECT logtime, sign, version"
                + " FROM url_parse_100";

        /** 输出数据到rocketmq */
        tableEnv.executeSql(printData);

    }

}
