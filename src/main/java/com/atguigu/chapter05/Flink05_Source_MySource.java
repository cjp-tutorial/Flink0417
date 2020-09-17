package com.atguigu.chapter05;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
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
public class Flink05_Source_MySource {
    public static void main(String[] args) throws Exception {
        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // TODO 1.Source：从自定义数据源读取
        DataStreamSource<WaterSensor> inputDS = env.addSource(new MySourceFunction());

        inputDS.print();

        env.execute();
    }


    /**
     * 自定义数据源
     * 1. 实现 SourceFunction，指定输出的类型
     * 2. 重写 两个方法
     * run():
     * cancel():
     */
    public static class MySourceFunction implements SourceFunction<WaterSensor> {
        // 定义一个标志位，控制数据的产生
        private boolean flag = true;

        @Override
        public void run(SourceContext<WaterSensor> ctx) throws Exception {
            Random random = new Random();
            while (flag) {
                ctx.collect(
                        new WaterSensor(
                                "sensor_" + random.nextInt(3),
                                System.currentTimeMillis(),
                                random.nextInt(10) + 40
                        )
                );
                Thread.sleep(2000L);
            }
        }

        @Override
        public void cancel() {
            this.flag = false;
        }
    }
}
