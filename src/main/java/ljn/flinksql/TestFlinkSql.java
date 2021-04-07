package ljn.flinksql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class TestFlinkSql {

    private static TableEnvironment tableEnv;

    public static void main(final String[] args) throws Exception {
        initBlinkEnv();
        registerFileSource();
        print();
    }

    private static void initBlinkEnv() {
        EnvironmentSettings tableEnvSettings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner() // 设置使用BlinkPlanner
                .inStreamingMode() // 设置流处理模式
                .build();

        tableEnv = TableEnvironment.create(tableEnvSettings);
    }

    private static void registerFileSource() {

        String sourceSql = "CREATE TABLE url_parse_100 ("
                + " logtime STRING,"
                + " sign STRING,"
                + " version STRING"
                + " ) WITH ( "
                + " 'connector' = 'rocketmq',"
                + " 'format' = 'json',"
                + " 'host' = '172.16.16.118',"
                + " 'port' = '9876',"
                + " 'topic' = 'PushTopic',"
                + " 'group' = 'ddd'"
                + ")";

        tableEnv.executeSql(sourceSql); // 注册source表到env中
    }

    /**
     * 注册print table并输出数据.
     */
    private static void print() {

        String printTable = "CREATE TABLE print_table"
                + " WITH ('connector' = 'rocketmq',"
                +"'format' = 'json',"
                + " 'host' = '172.16.16.119',"
                + " 'port' = '9876',"
                + " 'topic' = 'ljntest',"
                + " 'group' = 'ddd'"
                + ")"
                + " LIKE url_parse_100 (EXCLUDING ALL)"; // 注册print表到env中

        tableEnv.executeSql(printTable);

        String printData = "INSERT INTO print_table"
                + " SELECT logtime, sign, version"
                + " FROM url_parse_100";

        tableEnv.executeSql(printData); // 输出数据到控制台

    }

}
