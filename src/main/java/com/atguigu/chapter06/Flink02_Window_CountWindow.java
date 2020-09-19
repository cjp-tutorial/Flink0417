package com.atguigu.chapter06;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/9/19 11:22
 */
public class Flink02_Window_CountWindow {
    public static void main(String[] args) throws Exception {
        // 0 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.
        DataStreamSource<String> socketDS = env.socketTextStream("localhost", 9999);


        KeyedStream<Tuple2<String, Integer>, String> dataKS = socketDS
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        return Tuple2.of(value, 1);
                    }
                })
                .keyBy(r -> r.f0);


        // TODO CountWindow
        // 根据 本组 数据条数 => 因为是 keyby之后开的窗
        // 在滑动窗口中，一个数据能属于多少个窗口？ => 窗口长度 / 滑动步长
        // 在滑动窗口中，每经过一个滑动步长，就会触发一个窗口的计算
        dataKS
//                .countWindow(3) // 滚动窗口：一个参数，是窗口大小
                .countWindow(3,2) // 滑动窗口: 两个参数，第一个是窗口长度，第二个是滑动步长
                .sum(1)
                .print();

        env.execute();
    }
}
