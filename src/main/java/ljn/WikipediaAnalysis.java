package ljn;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;

public class WikipediaAnalysis {
    public static void main(String[] args) throws Exception {
        /** 创建一个streaming程序运行的上下文 */
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        /** source部分--数据来源部分-WikipediaEditsSource pom中封装好的数据输入 */
        DataStream<WikipediaEditEvent> edits = see.addSource(new WikipediaEditsSource());

        /** 监控一个时间段之内每一个用户，是否做了编辑并且编辑了多少字节，对词条修改了多少内容
         * （统计的是字节数） */
        /** event-wiki百科后台每一次的修改事件 */
        /** keyBy 逻辑上将流分区为不相交的分区，每个分区包含相同key的元素，返回一个KeyedDataStream */
        KeyedStream<WikipediaEditEvent, String> keyedEdits = edits
                /** KeySelector key的选择器 */
                .keyBy((KeySelector<WikipediaEditEvent, String>)  event->{
                    /** 从事件中get出用户 */
                   return event.getUser();
                });
        /** 5秒钟汇总一次 */
        DataStream<Tuple2<String, Long>> result = keyedEdits
                /** 指定窗口宽度为5秒 */
                .timeWindow(Time.seconds(5))
                /** fold聚合函数，第一个参数：初始值，第二个参数：FoldFunction */
                /** Tuple2<String 用户, Long 用户在5s的窗口内编辑的字节数> */
                /** flink发展非常快，为了保持向下兼容，方法并没有删除，只是不建议我们使用 */ 
                .fold(new Tuple2<>("", 0L), new FoldFunction<WikipediaEditEvent, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> fold(Tuple2<String, Long> acc, WikipediaEditEvent event) {
                        /** 赋给Tuple的第一个字段 */
                        acc.f0 = event.getUser();
                        /** 赋给Tuple的第二个字段
                         * （因为一个窗口中可能一个用户编辑了几条日志发送过来，
                         * 所以对同一个用户做一下聚合：
                         * 从event中把字节数的变化get出来，累加给acc.f1，并返回） */
                        acc.f1 += event.getByteDiff();
                        return acc;
                    }
                });
        /** 没有sink部分，直接做一个打印 */
        /** 真正生产环境，这里可能是发送到kafka中，或者落地到其他的存储中，如redis等等 */ 
        result.print();
        /** 直接用上下文进行执行 */
        see.execute();

    }
}
