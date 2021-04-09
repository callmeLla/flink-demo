package com.ljn.flinksql;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.ArrayList;
import java.util.List;


/**
 * @ClassName: WordCountSQLUDF
 * @Description: 通过flinksql实现wordcount并将结果加上.com (使用自定义函数处理特定场景的需求)
 * @Author: liujianing
 * @Date: 2021/4/7 16:11
 * @Version: v1.0 文件初始创建
 */
public class WordCountSQLUDF {
    public static void main(String[] args) throws Exception {
        /** 获取执行上下文 */
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        /** 获取table的上下文 */
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

        /** 将自定义函数注册到flink  参数1：函数名，参数2：具体函数 */
        tEnv.registerFunction("StringToSite",new StringToSite(".com"));

        List list = new ArrayList();
        /** Hello.com Flink.com ljn.com */
        String wordsStr = "Hello Flink Hello ljn";
        String[] words = wordsStr.split("\\W+");
        for (String word : words) {
            WC wc = new WC(word,1);
            list.add(wc);
        }
        DataSource<WC> input = env.fromCollection(list);
        /** DataSet 转sql，指定字段名 */
        Table table1 = tEnv.fromDataSet(input, "word,frequency");
        table1.printSchema();
        /** 注册为一个表 */
        tEnv.registerTable("WordCount", table1);
        String sql = "SELECT StringToSite(word) as word,SUM(frequency) as frequency FROM WordCount GROUP BY word";
        Table table2 = tEnv.sqlQuery(sql);
        /** 将表转换DaraSet */
        DataSet<WC> res = tEnv.toDataSet(table2, WC.class);
        res.print();
    }


    /**
     * @Description: 自定义函数StringToSite  hello->hello.com
     * @Date: 2021/4/9 16:59
     * @Author: liujianing
     * @Return
     * @Throws
     */
    public static class StringToSite extends ScalarFunction{

        private String address;

        public StringToSite(String address){
            this.address = address;
        }

        public String eval(String s){
            return s + address;
        }
    }


    /** 自定义实体，存放WordCount */
    public static class WC{
        /** 传进来的word Hello Flink Hello ljn */
        public String word;
        /** 统计出现的次数 */
        public long frequency;
        public WC(){}
        public WC(String word, long frequency){
            this.word = word;
            this.frequency = frequency;
        }

        @Override
        public String toString(){
            return word + "," + frequency;
        }

    }

}
