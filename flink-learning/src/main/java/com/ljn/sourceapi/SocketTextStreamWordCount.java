package com.ljn.sourceapi;

import com.ljn.WordCount;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName: SocketTextStreamWordCount
 * @Description: SourceAPI样例-只修改wordcount的获取数据部分
 * 从socket中获取数据(流式)
 * @Author: liujianing
 * @Date: 2021/4/7 17:47
 * @Version: v1.0 文件初始创建
 */
public class SocketTextStreamWordCount {
    public static void main(String[] args) throws Exception {
        /** 创建一个运行环境 */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /** 从socket获取数据-本机9001端口 需启动socket发送word */
        DataStreamSource<String> text = env.socketTextStream("127.0.0.1",9001);

        /** 对这个DataSet进行处理 */
        DataStream<Tuple2<String,Integer>> counts =
                /** flatMap-将一系列输入打散压平，自己实现LineSplitter方法 */
                text.flatMap(new LineSplitter())
                        /** （i,1）(am,1)(chinese,1) */
                        /** 按照key进行聚合，0代表第0个元素 */
                        .keyBy(0)
                        /** 按第一个元素求和 */
                        .sum(1);
        /** 进行打印 */
        counts.print().setParallelism(1);
        env.execute("Socket Window WordCount");

    }

    public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String,Integer>> {
        /** 复写flatMap实现功能 */
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            /** 转小写，用非单词字符进行分割，文本转换成String数组 */
            String[] tokens = value.toLowerCase().split("\\W+");
            /** 对数组进行遍历 */
            for (String token : tokens) {
                /** 过滤掉一些空格、空行 - 防御性编程 */
                if(token.length() > 0){
                    /** 把结果包装成key是token，value是1，收集成Tuple */
                    out.collect(new Tuple2<String, Integer>(token, 1));
                }
            }
        }
    }
}
