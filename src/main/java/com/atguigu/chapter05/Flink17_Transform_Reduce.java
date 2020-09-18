package com.atguigu.chapter05;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/9/16 15:29
 */
public class Flink17_Transform_Reduce {
    public static void main(String[] args) throws Exception {

        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.从文件读取数据
        DataStreamSource<String> inputDS = env
//                .readTextFile("input/sensor-data.log");
                .socketTextStream("localhost", 9999);

        // 2.Transform: Map转换成实体对象
        SingleOutputStreamOperator<WaterSensor> sensorDS = inputDS.map(new Flink06_Transform_Map.MyMapFunction());

        // 3.按照 id 分组
        KeyedStream<Tuple3<String, Long, Integer>, String> sensorKS = sensorDS
                .map(new MapFunction<WaterSensor, Tuple3<String, Long, Integer>>() {
                    @Override
                    public Tuple3<String, Long, Integer> map(WaterSensor value) throws Exception {
                        return new Tuple3<>(value.getId(), value.getTs(), value.getVc());
                    }
                })
                .keyBy(r -> r.f0);

        // TODO Reduce
        // 1.输入的类型要一致，输出的类型也要一致
        // 2.第一条来的数据，不会进入reduce
        // 3.帮我们保存了中间状态
        sensorKS
                .reduce(
                        new ReduceFunction<Tuple3<String, Long, Integer>>() {
                            @Override
                            public Tuple3<String, Long, Integer> reduce(Tuple3<String, Long, Integer> value1, Tuple3<String, Long, Integer> value2) throws Exception {
                                System.out.println(value1.toString() + " <-> " + value2.toString());
                                return Tuple3.of("aaa", 123L, value1.f2 + value2.f2);
                            }
                        }
                )
                .print("reduce");

        env.execute();
    }


    public static class MyKeySelector implements KeySelector<WaterSensor, String> {

        @Override
        public String getKey(WaterSensor value) throws Exception {
            return value.getId();
        }
    }


}
