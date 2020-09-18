package com.atguigu.chapter05;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/9/16 16:55
 */
public class Flink13_Transform_Select {
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

        // TODO Split: 水位低于 50 正常，水位 [50，80) 警告， 水位高于 80 告警
        // split并不是真正的把流分开
        SplitStream<WaterSensor> splitSS = sensorDS.split(new OutputSelector<WaterSensor>() {
                                                              @Override
                                                              public Iterable<String> select(WaterSensor value) {
                                                                  if (value.getVc() < 50) {
                                                                      return Arrays.asList("normal","happy","hi");
                                                                  } else if (value.getVc() < 80) {
                                                                      return Arrays.asList("warn","happy");
                                                                  } else {
                                                                      return Arrays.asList("alarm");
                                                                  }
                                                              }
                                                          }
        );

        //TODO select
        // 通过之前的标签名，获取对应的流
        // 一个流可以起多个名字，取出的时候，给定一个名字就行
        splitSS.select("normal").print("normal");
//        splitSS.select("hi").print("normal");
        splitSS.select("happy").print("warn");
//        splitSS.select("warn").print("warn");
//        splitSS.select("alarm").print("alarm");


        env.execute();
    }
}
