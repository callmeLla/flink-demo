package com.ljn.flinksql;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;

import java.util.ArrayList;
import java.util.List;


/**
 * @ClassName: WordCountSQL
 * @Description: 通过flinksql实现wordcount
 * @Author: liujianing
 * @Date: 2021/4/7 16:11
 * @Version: v1.0 文件初始创建
 */
public class WordCountSQL {
    public static void main(String[] args) throws Exception {
        /** 获取执行上下文 */
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        /** 获取table的上下文 */
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

        List list = new ArrayList();
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
        String sql = "SELECT word,SUM(frequency) as frequency FROM WordCount GROUP BY word";

        /** 比较函数（等于/like） */
        /** 找出单词是Hello的 */
        String sql1 = "SELECT word,SUM(frequency) as frequency FROM WordCount where word = 'Hello' GROUP BY word";
        /** 找出单词中包含i的 */
        String sql2 = "SELECT word,SUM(frequency) as frequency FROM WordCount where word like '%i%' GROUP BY word";

        /** 算数函数（+/-） */
        /** 出现的频率基础上加一 */
        String sql3 = "SELECT word,SUM(frequency)+1 as frequency FROM WordCount GROUP BY word";

        /** 注意：在sql中进行函数处理的时候一定要as重命名，不然会报错 */
        /** 字符串处理函数（UPPER/LOWER/SUBSTRING）*/
        /** 小写转大写 */
        String sql4 = "SELECT UPPER(word) as word,SUM(frequency) as frequency FROM WordCount GROUP BY word";
        /** 去掉第一个字母 SUBSTRING函数支持多个参数(第二个参数：从第几个开始截取，第三个参数：截取几个，如果不传，直接截取到最后) */
        String sql5 = "SELECT SUBSTRING(word,2,3) as word,SUM(frequency) as frequency FROM WordCount GROUP BY word";


        /** 其他函数（MD5） */
        String sql6 = "SELECT MD5(word) as word,SUM(frequency) as frequency FROM WordCount GROUP BY word";

        Table table2 = tEnv.sqlQuery(sql5);
        /** 将表转换DaraSet */
        DataSet<WC> res = tEnv.toDataSet(table2, WC.class);
        res.print();
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
