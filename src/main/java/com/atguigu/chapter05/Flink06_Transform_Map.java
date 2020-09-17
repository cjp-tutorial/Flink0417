package com.atguigu.chapter05;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/9/16 15:29
 */
public class Flink06_Transform_Map {
    public static void main(String[] args) throws Exception {
        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 1.从文件读取数据
        DataStreamSource<String> inputDS = env.readTextFile("input/sensor-data.log");

        // 2.Transform: Map转换成实体对象
        SingleOutputStreamOperator<WaterSensor> sensorDS = inputDS.map(new MyMapFunction());

        // 3.打印
        sensorDS.print();

        env.execute();
    }

    /**
     * 实现MapFunction，指定输入的类型，返回的类型
     * 重写 map方法
     */
    public static class MyMapFunction implements MapFunction<String, WaterSensor> {

        @Override
        public WaterSensor map(String value) throws Exception {
            String[] datas = value.split(",");
            return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
        }
    }
}
