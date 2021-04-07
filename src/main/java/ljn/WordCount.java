package ljn;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


public class WordCount {
    public static void main(String[] args) throws Exception {
        /** 创立一个flink执行环境的上下文ExecutionEnvironment */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        /** 使用本地的几行文本作为需要处理的数据 */
        DataSet<String> text = env.fromElements("to be, or not to be,--that is the question:--",
                "wherether 'tis nobler in the mind to suffer",
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "bb cc dd ee dfdddd aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");

        /** 对这个DataSet进行处理 */
        DataSet<Tuple2<String,Integer>> counts =
                text.flatMap(new LineSplitter())
                        /** （i,1）(am,1)(chinese,1) */
                        /** 按照key进行聚合，0代表第0个元素 */
                        .groupBy(0)
                        /** 按第一个元素求和 */
                        .sum(1);
        /** 进行打印 */
        counts.print();
    }


    /**
     * @Description:  自己实现的统计wordcount方法：
     * 把整个句子用非单词符号切分开，分成一个一个的单词
     * 拼装成单词一个一个的Tuple
     * 计算每一个Tuple单词的个数进行聚合
     * @Date: 2021/4/2 17:16
     * @Author: liujianing
     * @Return
     * @Throws
     */
    public static final class LineSplitter implements FlatMapFunction<String,Tuple2<String,Integer>> {
        /** 复写flatMap实现功能 */
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            /** 转小写，用非单词字符进行分割，文本转换成String数组 */
            String[] tokens = value.toLowerCase().split("\\W+");
            /** 对数组进行遍历 */
            for (String token : tokens) {
                /** 过滤掉一些空格、空行 */
                if(token.length() > 0){
                    /** 把结果包装成key是token，value是1，收集成Tuple */
                    out.collect(new Tuple2<String, Integer>(token, 1));
                }
            }
        }
    }
}
