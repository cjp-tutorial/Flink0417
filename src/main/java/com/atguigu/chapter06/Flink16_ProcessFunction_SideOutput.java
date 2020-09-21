package com.atguigu.chapter06;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import javax.annotation.Nullable;

/**
 * 水位高于 阈值 通过 侧输出流 告警
 *
 * @author cjp
 * @version 1.0
 * @date 2020/9/19 11:22
 */
public class Flink16_ProcessFunction_SideOutput {
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

        //TODO 使用侧输出流
        // 1.定义一个OutputTag，给定一个 名称
        // 2.使用 ctx.output(outputTag对象,放入侧输出流的数据)
        // 3.获取侧输出流 => DataStream.getSideOutput(outputTag对象)
        OutputTag<String> outputTag = new OutputTag<String>("vc alarm"){};

        SingleOutputStreamOperator<WaterSensor> processDS = sensorDS
                .keyBy(data -> data.getId())
                .process(
                        new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {

                            /**
                             * 来一条数据，处理一条
                             * @param value
                             * @param ctx
                             * @param out
                             * @throws Exception
                             */
                            @Override
                            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                               if (value.getVc() > 5){
                                   // 水位高于阈值，用侧输出流告警
                                   ctx.output(outputTag, "水位高于阈值5！！！");
                               }
                               out.collect(value);
                            }
                        }
                );


        processDS.print();
        processDS.getSideOutput(outputTag).print("alarm");

        env.execute();
    }
}
