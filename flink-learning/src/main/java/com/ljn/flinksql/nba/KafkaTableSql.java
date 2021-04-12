package com.ljn.flinksql.nba;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Properties;

/**
 * @ClassName: KafkaTableSql
 * @Description: kafka结合flinksql
 * @Author: liujianing
 * @Date: 2021/4/12 18:47
 * @Version: v1.0 文件初始创建
 */
public class KafkaTableSql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        /** test 的 kafka topic，Flink-kafka */
        /** 泛型：向kafka中发送消息的序列化的形式 */
        /** 三个参数：队列名（topic），解码器，连接kafka的参数 */
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), properties);
        /** 从topic的最起始位置进行消费 */
        consumer.setStartFromEarliest();
        /** 将kafka数据放入DataStreamSource */
        DataStreamSource<String> topic = env.addSource(consumer);
        /** MapFunction泛型两个参数：1、发送来的数据类型，2、转换后的数据类型 */
        SingleOutputStreamOperator<String> kafkaSource = topic.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                return s;
            }
        });
        /** 注册内存表 1、表名，2、数据，3、列名 */
        tEnv.registerDataStream("books", kafkaSource, "name");
        /** SQL： select name，count(1) from books group by name */
        Table result = tEnv.sqlQuery("select name, count(1) from books group by name");
        /** 非常重要的知识点：回退更新  java1--》java2 （第一个人买了java，第二个人买了数据结构，第三个人买了java，
         * 这时java的购买数量要变成2）--实时大屏的实现方式 */
        /** 两个参数：1、结果，2、固定写法Row.class */
        tEnv.toRetractStream(result, Row.class).print();
        /** 进行提交 */
        env.execute();

    }
}
