package com.ljn.flinksql.nba;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;

public class NbaTableSql {


    public static void main(String[] args) throws Exception{
        /** 1、获取上下文环境 table环境 */
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        /** 有界数据集table上下文环境 */
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);
        /** 2、读取score.csv */
        DataSet<String> input =  env.readTextFile("D:\\works\\flink-demo\\flink-learning\\src\\main\\java\\com\\ljn\\flinksql\\nba\\score.csv");
        /** 打印读到的数据方便调试 */
        input.print();
        /** 把每一行数据处理成PlayerData然后返回 */
        DataSet<PlayerData> topInput = input.map(new MapFunction<String, PlayerData>() {
            @Override
            public PlayerData map(String s) throws Exception {
                /** 每个s就是score.csv中的一行数据 */
                String[] split = s.split(",");
                return new PlayerData(String.valueOf(split[0]),
                        String.valueOf(split[1]),
                        String.valueOf(split[2]),
                        Integer.valueOf(split[3]),
                        Double.valueOf(split[4]),
                        Double.valueOf(split[5]),
                        Double.valueOf(split[6]),
                        Double.valueOf(split[7]),
                        Double.valueOf(split[8]));
            }
        });
        /** 3、注册成内存表 */
        Table topScore = tEnv.fromDataSet(topInput);
        tEnv.registerTable("score", topScore);

        /** 4、编写sql 然后提交执行 */
        /** select player,count(season) as num from score group by player order by num desc */
        String sql = "select player,count(season) as num from score group by player order by num desc";
        Table queryResult = tEnv.sqlQuery(sql);

        /** 5、结果进行打印 */
        /** 把结果转成DataSet */
        DataSet<Result> result = tEnv.toDataSet(queryResult, Result.class);
        result.print();

    }




    /**
     * @ClassName: PlayerData
     * @Description: 球员得分数据
     * @Author: liujianing
     * @Date: 2021/4/12 11:17
     * @Version: v1.0 文件初始创建
     */
    public static class PlayerData{
        /** 赛季 */
        public String season;
        /** 球员 */
        public String player;
        /** 出场 */
        public String play_num;
        /** 首发 */
        public Integer first_court;
        /** 时间 */
        public Double time;
        /** 助攻 */
        public Double assists;
        /** 抢断 */
        public Double steals;
        /** 盖帽 */
        public Double blocks;
        /** 得分 */
        public Double scores;

        public PlayerData(){
            super();
        }

        public PlayerData(String season,
                          String player,
                          String play_num,
                          Integer first_court,
                          Double time,
                          Double assists,
                          Double steals,
                          Double blocks,
                          Double scores){
            this.season = season;
            this.player = player;
            this.play_num = play_num;
            this.first_court = first_court;
            this.time = time;
            this.assists = assists;
            this.steals = steals;
            this.blocks = blocks;
            this.scores = scores;
        }

    }

    /**
     * @ClassName: Result
     * @Description: 结果数据
     * @Author: liujianing
     * @Date: 2021/4/12 11:18
     * @Version: v1.0 文件初始创建
     */
    public static class Result{
        /** 球员 */
        public String player;
        /** 得分王次数 */
        public Long num;

        public Result(){
            super();
        }
        public Result(String player, Long num){
            this.player = player;
            this.num = num;
        }

        @Override
        public String toString(){
            return player + ":" + num;
        }

    }

}
