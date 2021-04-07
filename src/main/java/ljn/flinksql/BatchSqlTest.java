package ljn.flinksql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Created by search-lemon on 2020/10/27.
 * 读取本地url_parse_100.txt文件，解析后输出到控制台.
 */
public class BatchSqlTest {

    // use blink planner
    private static TableEnvironment tableEnv;

    public static void main(String[] args) {
        initBlinkEnv();
        registerFileSource();
        print();
    }

    private static void initBlinkEnv() {
        EnvironmentSettings tableEnvSettings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner() // 设置使用BlinkPlanner
                .inBatchMode() // 设置批处理模式
                .build();

        tableEnv = TableEnvironment.create(tableEnvSettings);
    }

    private static void registerFileSource() {

        String sourceSql = "CREATE TABLE url_parse_100 ("
                + " logtime STRING,"
                + " sign STRING,"
                + " version STRING"
                + " ) WITH ( "
                + " 'connector' = 'filesystem',"
                + " 'path' = 'D:\\test\\flink\\url_parse_100.txt',"
                + " 'format' = 'json'"
                + ")";

        tableEnv.executeSql(sourceSql); // 注册source表到env中
    }

    /**
     * 注册print table并输出数据.
     */
    private static void print() {

        String printTable = "CREATE TABLE print_table"
                + " WITH ('connector' = 'print')"
                + " LIKE url_parse_100 (EXCLUDING ALL)"; // 注册print表到env中

        tableEnv.executeSql(printTable);

        String printData = "INSERT INTO print_table"
                + " SELECT logtime, sign, version"
                + " FROM url_parse_100";

        tableEnv.executeSql(printData); // 输出数据到控制台

    }
}
