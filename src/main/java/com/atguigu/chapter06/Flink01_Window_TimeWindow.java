package com.atguigu.chapter06;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/9/19 11:22
 */
public class Flink01_Window_TimeWindow {
    public static void main(String[] args) throws Exception {
        // 0 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.
        DataStreamSource<String> socketDS = env.socketTextStream("localhost", 9999);

        // TODO 开窗
        // DataStream可以直接调用开窗的方法，但是都带"all",这种情况下所有 数据不分组，都在窗口里
//        socketDS.windowAll();
//        socketDS.countWindowAll();
//        socketDS.timeWindowAll();


        KeyedStream<Tuple2<String, Integer>, String> dataKS = socketDS
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        return Tuple2.of(value, 1);
                    }
                })
                .keyBy(r -> r.f0);


        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> dataWS = dataKS
                .timeWindow(Time.seconds(5)); // 滚动窗口 => 传一个参数： 窗口长度
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)));
//                .timeWindow(Time.seconds(5), Time.seconds(2)); // 滑动窗口 => 传两个参数： 第一个是 窗口长度 ； 第二个是 滑动步长
//                .window(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(2));
//                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(3)));
        dataWS.sum(1).print();

        env.execute();
    }
}
