package com.atguigu.chapter02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * wordcount-有界流：文件
 *
 * @author cjp
 * @version 1.0
 * @date 2020/9/15 10:31
 */
public class Flink02_WC_BoundedStream {
    public static void main(String[] args) throws Exception {
        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1.读取数据
        DataStreamSource<String> fileDS = env.readTextFile("/opt/module/data/word.txt");

        // 2.处理数据
        // 2.1 扁平化操作：切分、转换成二元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOneTuple = fileDS.flatMap(new MyFlatMapFunction());

        // 2.2 按照 word分组
        KeyedStream<Tuple2<String, Integer>, Tuple> wordAndOneKS = wordAndOneTuple.keyBy(0);

        // 2.3 按照分组进行聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = wordAndOneKS.sum(1);

        // 3.输出、保存
        result.print();

        // 4.启动
        env.execute();
    }

    public static class MyFlatMapFunction implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            // 1.切分
            String[] words = value.split(" ");
            for (String word : words) {
                // 2.转换成二元组
                Tuple2<String, Integer> tuple = Tuple2.of(word, 1);
                // 3.采集器向下游发送数据
                out.collect(tuple);
            }

        }
    }
}
