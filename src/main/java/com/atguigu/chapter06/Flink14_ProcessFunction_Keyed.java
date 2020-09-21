package com.atguigu.chapter06;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.sql.Timestamp;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/9/19 11:22
 */
public class Flink14_ProcessFunction_Keyed {
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
                .assignTimestampsAndWatermarks(
//                        new AscendingTimestampExtractor<WaterSensor>() {
//                            @Override
//                            public long extractAscendingTimestamp(WaterSensor element) {
//                                return element.getTs() * 1000L;
//                            }
//                        }
                        new AssignerWithPunctuatedWatermarks<WaterSensor>() {
                            private Long maxTs = Long.MIN_VALUE;

                            @Nullable
                            @Override
                            public Watermark checkAndGetNextWatermark(WaterSensor lastElement, long extractedTimestamp) {
                                maxTs = Math.max(maxTs, extractedTimestamp);
                                return new Watermark(maxTs);
                            }

                            @Override
                            public long extractTimestamp(WaterSensor element, long previousElementTimestamp) {
                                return element.getTs() * 1000L;
                            }
                        }
                );


        SingleOutputStreamOperator<Long> processDS = sensorDS

                .keyBy(data -> data.getId())
                .process(
                        new KeyedProcessFunction<String, WaterSensor, Long>() {

                            private Long triggerTs = 0L;

                            /**
                             * 来一条数据，处理一条
                             * @param value
                             * @param ctx
                             * @param out
                             * @throws Exception
                             */
                            @Override
                            public void processElement(WaterSensor value, Context ctx, Collector<Long> out) throws Exception {
                                // 可以获取当前数据的分组
//                                ctx.getCurrentKey();
                                //  可以获取当前数据代表的时间戳
//                                ctx.timestamp();
                                // 可以将数据放入侧输出流
//                                ctx.output(, );
                                // 定时器: 注册、删除、当前时间、当前watermark
//                                ctx.timerService().registerProcessingTimeTimer(
//                                        ctx.timerService().currentProcessingTime() + 5000L
//                                );

                                // 为了避免重复注册、重复创建对象，注册定时器的时候，判断一下是否已经注册过了
                                if (triggerTs == 0) {
                                    ctx.timerService().registerEventTimeTimer(
                                            value.getTs() * 1000L + 5000L
                                    );
                                    triggerTs = value.getTs() * 1000L + 5000L;
                                }
//                                ctx.timerService().deleteProcessingTimeTimer();
//                                ctx.timerService().deleteEventTimeTimer();
//                                ctx.timerService().currentProcessingTime();
//                                ctx.timerService().currentWatermark();

                            }

                            /**
                             * 到了定时的时间，要干什么
                             * @param timestamp 注册的定时器的时间
                             * @param ctx   上下文
                             * @param out   采集器
                             * @throws Exception
                             */
                            @Override
                            public void onTimer(long timestamp, OnTimerContext ctx, Collector<Long> out) throws Exception {
//                                System.out.println(new Timestamp(timestamp) + "定时器触发");
                                System.out.println(timestamp + "定时器触发");
                            }
                        }
                );


        processDS.print();

        env.execute();
    }
}
