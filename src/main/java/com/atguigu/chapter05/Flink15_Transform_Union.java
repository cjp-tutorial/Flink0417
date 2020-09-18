package com.atguigu.chapter05;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.Arrays;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/9/16 16:55
 */
public class Flink15_Transform_Union {
    public static void main(String[] args) throws Exception {
        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        // 获取流
        DataStreamSource<Integer> numDS = env.fromCollection(Arrays.asList(1, 2, 3, 4));
        DataStreamSource<Integer> numDS1 = env.fromCollection(Arrays.asList(11, 12, 13, 14));
        DataStreamSource<Integer> numDS2 = env.fromCollection(Arrays.asList(21, 22, 23, 24));

        //TODO Union连接流
        // 要求流的 数据类型 要相同
        // 可以连接多条流
        DataStream<Integer> unionDS = numDS
                .union(numDS1)
                .union(numDS2);

        unionDS
                .map(new MapFunction<Integer, Integer>() {
                    @Override
                    public Integer map(Integer value) throws Exception {
                        return value * 10;
                    }
                })
                .print("union");


        env.execute();
    }
}
