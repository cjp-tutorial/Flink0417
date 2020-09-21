package com.atguigu.chapter05;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.Arrays;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/9/16 16:55
 */
public class Flink14_Transform_Connect {
    public static void main(String[] args) throws Exception {
        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.从文件读取数据
        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .readTextFile("input/sensor-data.log")
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
                    }
                });

        // 再获取一条流
        DataStreamSource<Integer> numDS = env.fromCollection(Arrays.asList(1, 2, 3, 4));

        // TODO 使用connect连接两条流
        // 两条流 数据类型 可以不一样
        // 只能两条流进行连接
        // 处理数据的时候，也是分开处理
        ConnectedStreams<WaterSensor, Integer> sensorNumCS = sensorDS.connect(numDS);

        // 调用其他算子
        SingleOutputStreamOperator<Object> resultDS = sensorNumCS.map(
                new CoMapFunction<WaterSensor, Integer, Object>() {

                    @Override
                    public String map1(WaterSensor value) throws Exception {
                        return value.toString();
                    }

                    @Override
                    public Integer map2(Integer value) throws Exception {
                        return value + 10;
                    }
                });

        resultDS.print();


        env.execute();
    }
}
