package com.atguigu.chapter05;

import com.atguigu.bean.AdClickLog;
import com.atguigu.bean.MarketingUserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * 不同省份、不同广告的点击量 实时统计
 *
 * @author cjp
 * @version 1.0
 * @date 2020/9/18 16:32
 */
public class Flink29_Case_AdClickAnalysis {
    public static void main(String[] args) throws Exception {
        // 0 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.读取数据，转成bean对象
        SingleOutputStreamOperator<AdClickLog> adClickDS = env
                .readTextFile("input/AdClickLog.csv")
                .map(new MapFunction<String, AdClickLog>() {
                    @Override
                    public AdClickLog map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new AdClickLog(
                                Long.valueOf(datas[0]),
                                Long.valueOf(datas[1]),
                                datas[2],
                                datas[3],
                                Long.valueOf(datas[4])
                        );
                    }
                });

        // 2.处理数据：不同省份、不同广告的点击量 实时统计
        // 2.1 按照 统计维度 分组： 省份、广告
        adClickDS
                .map(new MapFunction<AdClickLog, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(AdClickLog value) throws Exception {
                        return Tuple2.of(value.getProvince() + "_" + value.getAdId(), 1);
                    }
                })
                .keyBy(data -> data.f0)
                .sum(1)
                .print("ad click analysis");


        env.execute();
    }


}
