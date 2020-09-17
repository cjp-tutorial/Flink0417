package com.atguigu.chapter05;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/9/16 15:29
 */
public class Flink07_Transform_RichMapFunction {
    public static void main(String[] args) throws Exception {

        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 1.从文件读取数据
        DataStreamSource<String> inputDS = env.readTextFile("input/sensor-data.log");
//        DataStreamSource<String> inputDS = env.socketTextStream("localhost",9999);

        // 2.Transform: Map转换成实体对象
        SingleOutputStreamOperator<WaterSensor> sensorDS = inputDS.map(new MyRichMapFunction());

        // 3.打印
        sensorDS.print();

        env.execute();
    }

    /**
     * 继承 RichMapFunction，指定输入的类型，返回的类型
     * 提供了 open()和 close() 生命周期管理方法
     * 能够获取 运行时上下文对象 =》 可以获取 状态、任务信息 等环境信息
     */
    public static class MyRichMapFunction extends RichMapFunction<String, WaterSensor> {
        @Override
        public WaterSensor map(String value) throws Exception {
            String[] datas = value.split(",");
            return new WaterSensor(getRuntimeContext().getTaskName() + datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
        }


        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("open...");
        }

        @Override
        public void close() throws Exception {
            System.out.println("close...");
        }
    }
}
