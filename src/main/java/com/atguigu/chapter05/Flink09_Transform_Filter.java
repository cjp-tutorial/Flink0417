package com.atguigu.chapter05;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/9/16 15:29
 */
public class Flink09_Transform_Filter {
    public static void main(String[] args) throws Exception {
        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 1.从文件读取数据
        DataStreamSource<Integer> inputDS = env.fromCollection(
                Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8)
        );

        // TODO Transform: filter => 为 true保留，为 false丢弃
        inputDS
                .filter(new MyFilterFunction())
                .print();

        env.execute();
    }

    public static class MyFilterFunction implements FilterFunction<Integer> {

        @Override
        public boolean filter(Integer value) throws Exception {
            return value % 2 == 0;
        }
    }
}
