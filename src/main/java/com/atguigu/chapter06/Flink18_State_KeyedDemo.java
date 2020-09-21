package com.atguigu.chapter06;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * 水位高于 阈值 通过 侧输出流 告警
 *
 * @author cjp
 * @version 1.0
 * @date 2020/9/19 11:22
 */
public class Flink18_State_KeyedDemo {
    public static void main(String[] args) throws Exception {
        // 0 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // TODO 1.env指定时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 1.
        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("localhost", 9999)
                .map(new MyMapFunction())
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


        SingleOutputStreamOperator<WaterSensor> processDS = sensorDS
                .keyBy(data -> data.getId())
                .process(
                        new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {
                            // 1.定义状态
                            ValueState<Integer> valueState;
                            ListState<String> listState;
                            MapState<String, String> mapState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                // 2.在 open里面创建状态
                                valueState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("valueState", Integer.class, 0));
                                listState = getRuntimeContext().getListState(new ListStateDescriptor<String>("listState", String.class));
                                mapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, String>("mapState", String.class, String.class));
                            }

                            @Override
                            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
//                                valueState.value(); // 取出值
//                                valueState.update(); // 更新值
//                                valueState.clear();

//                                listState.add();    // 添加单个值
//                                listState.addAll(); //添加整个List
//                                Iterator<String> iterator = listState.get().iterator();
//                                while(iterator.hasNext()){
//                                    iterator.next();
//                                }
//                                listState.update(); // 更新整个集合
//                                listState.clear();

//                                mapState.put(); // 添加单个 k-v 对
//                                mapState.putAll();  // 添加整个Map
//                                mapState.get(); // 根据key，获取value
//                                mapState.contains();
//                                mapState.remove();
//                                mapState.clear();

                            }
                        }
                );


        processDS.print();


        env.execute();
    }


    public static class MyMapFunction implements MapFunction<String, WaterSensor>, ListCheckpointed<String> {
        @Override
        public WaterSensor map(String value) throws Exception {
            String[] datas = value.split(",");
            return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));

        }

        @Override
        public List<String> snapshotState(long checkpointId, long timestamp) throws Exception {
            return null;
        }

        @Override
        public void restoreState(List<String> state) throws Exception {

        }
    }
}
