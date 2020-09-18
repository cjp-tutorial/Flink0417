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
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/9/16 15:29
 */
public class Flink18_Transform_Process {
    public static void main(String[] args) throws Exception {

        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.从文件读取数据
        DataStreamSource<String> inputDS = env
                .readTextFile("input/sensor-data.log");
//                .socketTextStream("localhost", 9999);

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

        // TODO Process
        // 可以获取到一些 环境信息
        sensorKS.process(
                new KeyedProcessFunction<String, Tuple3<String, Long, Integer>, String>() {
                    /**
                     * 处理数据的方法：来一条处理一条
                     * @param value 一条数据
                     * @param ctx   上下文
                     * @param out   采集器
                     * @throws Exception
                     */
                    @Override
                    public void processElement(Tuple3<String, Long, Integer> value, Context ctx, Collector<String> out) throws Exception {
                        out.collect("当前key=" + ctx.getCurrentKey() + "当前时间=" + ctx.timestamp() + ",数据=" + value);
                    }
                }
        )
                .print("process");


        env.execute();
    }


    public static class MyKeySelector implements KeySelector<WaterSensor, String> {

        @Override
        public String getKey(WaterSensor value) throws Exception {
            return value.getId();
        }
    }


}
