package com.atguigu.chapter07;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

import java.util.List;
import java.util.Map;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/9/25 9:24
 */
public class Flink02_CEP_API {
    public static void main(String[] args) throws Exception {
        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 1.从文件读取数据
        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .readTextFile("input/sensor-data-cep.log")
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
                    }
                })
                .assignTimestampsAndWatermarks(
                        new AscendingTimestampExtractor<WaterSensor>() {
                            @Override
                            public long extractAscendingTimestamp(WaterSensor element) {
                                return element.getTs() * 1000L;
                            }
                        }
                );

        // TODO 使用 CEP
        // 1.定义规则
        // 严格近邻 => 紧挨着、紧跟着，中间不能有 小三 插足 =》支持乱序的处理
        // 宽松近邻 => 只要后面跟着就行，不需要紧挨着 => 可以有小三，类似 一夫一妻
        // 非确定性宽松近邻 => 跟宽松近邻的区别：匹配上之后，还会接着匹配 => 贪心的，类似 一夫多妻 => 小李飞刀
        Pattern<WaterSensor, WaterSensor> pattern = Pattern
                .<WaterSensor>begin("start")
                .where(new IterativeCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor waterSensor, Context<WaterSensor> context) throws Exception {
                        return "sensor_1".equals(waterSensor.getId());
                    }
                })
//                .next("next")
//                .followedBy("followedBy")
                .followedByAny("followedByAny")
                .where(new IterativeCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor waterSensor, Context<WaterSensor> context) throws Exception {
                        return "sensor_1".equals(waterSensor.getId());
                    }
                });

        // 2.使用规则
        PatternStream<WaterSensor> sensorPS = CEP.pattern(sensorDS, pattern);

        // 3.取出匹配的结果
        SingleOutputStreamOperator<String> selectDS = sensorPS.select(
                new PatternSelectFunction<WaterSensor, String>() {
                    @Override
                    public String select(Map<String, List<WaterSensor>> map) throws Exception {
                        String start = map.get("start").toString();
//                        String next = map.get("next").toString();
//                        String followedBy = map.get("followedBy").toString();
                        String followedByAny = map.get("followedByAny").toString();
//                        return start + "-->" + next;
//                        return start + "-->" + followedBy;
                        return start + "-->" + followedByAny;
                    }
                }
        );

        selectDS.print("cep");

        env.execute();
    }
}
