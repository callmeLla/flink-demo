package com.ljn.flinksql;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;

/**
 * @ClassName: OuterJoinTest
 * @Description: 外连接测试
 * @Author: liujianing
 * @Date: 2021/4/9 16:30
 * @Version: v1.0 文件初始创建
 */
public class OuterJoinTest {

    public static void main(String[] args) throws Exception {
        /** 获取运行环境 */
        ExecutionEnvironment env =  ExecutionEnvironment.getExecutionEnvironment();

        /** Tuple2<用户ID, 用户姓名> */
        ArrayList<Tuple2<Integer, String>> data1 = new ArrayList<>();
        data1.add(new Tuple2<>(1, "小李"));
        data1.add(new Tuple2<>(2, "小王"));
        data1.add(new Tuple2<>(3, "小张"));
        /** Tuple2<用户ID, 用户所在城市> */
        ArrayList<Tuple2<Integer, String>> data2 = new ArrayList<>();
        data2.add(new Tuple2<>(1, "beijing"));
        data2.add(new Tuple2<>(2, "shanghai"));
        data2.add(new Tuple2<>(4, "guangzhou"));

        /** 将两个数据集加载到flink中 */
        DataSource<Tuple2<Integer, String>> text1 = env.fromCollection(data1);
        DataSource<Tuple2<Integer, String>> text2 = env.fromCollection(data2);

        /** 左外连接;以左表为准，右表可能不存在一些数据
         * 所以：second这个tuple中的元素可能为null
         * JoinFunction这个函数就是来处理这种问题的*/
        text1.leftOuterJoin(text2)
                /** 第一个表的第一列 */
                .where(0)
                /** 等于第二个表的第一列 */
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Object>() {
                    @Override
                    public Object join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                        if(second == null){
                            /** 当右表为null，返回用户ID,用户姓名和null */
                            return new Tuple3<>(first.f0,first.f1,"null");
                        }else{
                            /** 当右表不为null，返回用户ID,用户姓名和用户所在城市 */
                            return new Tuple3<>(first.f0,first.f1,second.f1);
                        }
                    }
                })
                .print();

    }

}
