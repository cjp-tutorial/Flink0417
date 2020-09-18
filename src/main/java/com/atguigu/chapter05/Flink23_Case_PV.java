package com.atguigu.chapter05;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/9/16 15:29
 */
public class Flink23_Case_PV {
    public static void main(String[] args) throws Exception {

        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.从文件读取数据、转换成 bean对象
        SingleOutputStreamOperator<UserBehavior> userBehaviorDS = env
                .readTextFile("input/UserBehavior.csv")
                .map(new MapFunction<String, UserBehavior>() {

                    @Override
                    public UserBehavior map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new UserBehavior(
                                Long.valueOf(datas[0]),
                                Long.valueOf(datas[1]),
                                Integer.valueOf(datas[2]),
                                datas[3],
                                Long.valueOf(datas[4])
                        );
                    }
                });

        // TODO 参考WordCount思路，实现 PV的统计
        // 2.处理数据
        // 2.1 过滤出 pv 行为
        SingleOutputStreamOperator<UserBehavior> userBehaviorFilter = userBehaviorDS.filter(data -> "pv".equals(data.getBehavior()));
        // 2.2 转换成 二元组 (pv,1) => 只关心pv行为，所以第一个元素给定一个写死的 "pv"
        SingleOutputStreamOperator<Tuple2<String, Integer>> pvAndOneTuple2 = userBehaviorFilter.map(new MapFunction<UserBehavior, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(UserBehavior value) throws Exception {
                return Tuple2.of("pv", 1);
            }
        });

        // 2.3 按照第一个位置的元素 分组 => 聚合算子只能在分组之后调用，也就是 keyedStream才能调用 sum
        KeyedStream<Tuple2<String, Integer>, Tuple> pvAndOneKS = pvAndOneTuple2.keyBy(0);

        // 2.4 求和
        SingleOutputStreamOperator<Tuple2<String, Integer>> pvDS = pvAndOneKS.sum(1);

        // 3.打印
        pvDS.print("pv");

        env.execute();
    }

}
