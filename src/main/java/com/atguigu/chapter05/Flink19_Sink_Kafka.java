package com.atguigu.chapter05;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.util.Collector;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/9/16 15:29
 */
public class Flink19_Sink_Kafka {
    public static void main(String[] args) throws Exception {

        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.从文件读取数据
        DataStreamSource<String> inputDS = env
                .readTextFile("input/sensor-data.log");
//                .socketTextStream("localhost", 9999);


        //TODO 数据 Sink到Kafka
        // DataStream调用 addSink =》 注意，不是env来调用
        inputDS.addSink(
                new FlinkKafkaProducer011<String>(
                        "hadoop102:9092",
                        "sensor0421",
                        new SimpleStringSchema())
        );


        env.execute();
    }


    public static class MyKeySelector implements KeySelector<WaterSensor, String> {

        @Override
        public String getKey(WaterSensor value) throws Exception {
            return value.getId();
        }
    }


}
