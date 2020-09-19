package com.atguigu.chapter06;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/9/19 11:22
 */
public class Flink07_Watermark_OutOfOrderness {
    public static void main(String[] args) throws Exception {
        // 0 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 1.env指定时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 1.
        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("localhost", 9999)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));

                    }
                })
                // TODO 2.指定如何 从数据中 抽取出 事件时间，时间单位是 ms
                // 官方提供了一个 BoundedOutOfOrdernessTimestampExtractor 乱序下 提取 事件时间 和 生成 watermark的 抽象类
                // 第一个，需要 重写 extractTimestamp => 如何 从数据中 抽取出 事件时间
                // 第二个，传参 => 最大乱序程序，是一个等待时间


                // 乱序 => 时间大的先到了，
                // 假设数据是 1，2，3，4，5，6 秒生成的，开3s的滚动窗口 [0,3),[3,6),[6,9)
                // 来的数据是 1，6，3，2，4，5 =》 最大乱序程度是 4s
                // => 等4s再关窗 => [0,3) 本应该在 ET >= 3s 时关窗 =》 等待之后，就是 7s 关窗
                // => Watermark表示时间进展、触发窗口的计算、关窗 => 也就是说 wm = 3s时，[0,3)关闭并计算
                // => watermark = EventTime - awaitTime = 7 - 4 = 3s
                // => 为了单调递增 ，上面公式的 EventTime，应该是当前为止，最大的时间戳

                // 最好是 等待多久？ => 最大乱序时间
                // 工作中，最大乱序时间 => 对数据进行抽样、进行估算 => 靠经验
                // 如果数据乱序程度达到 1小时 => 真的要等 1个小时吗？

                .assignTimestampsAndWatermarks(
                        new BoundedOutOfOrdernessTimestampExtractor<WaterSensor>(Time.seconds(3)){
                            @Override
                            public long extractTimestamp(WaterSensor element) {
                                return element.getTs() * 1000L;
                            }
                        }
                );


        // 分组、开窗、聚合
        sensorDS
                .keyBy(data -> data.getId())
                .timeWindow(Time.seconds(5))
                .process(
                        /**
                         * 全窗口函数：整个窗口的本组数据，存起来，关窗的时候一次性一起计算
                         */
                        new ProcessWindowFunction<WaterSensor, Long, String, TimeWindow>() {

                            @Override
                            public void process(String s, Context context, Iterable<WaterSensor> elements, Collector<Long> out) throws Exception {
                                out.collect(elements.spliterator().estimateSize());
                            }
                        })
                .print();


        env.execute();
    }
}
