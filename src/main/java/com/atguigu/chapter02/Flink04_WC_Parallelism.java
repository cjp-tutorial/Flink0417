package com.atguigu.chapter02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 并行度
 *
 *
 * @author cjp
 * @version 1.0
 * @date 2020/9/15 11:09
 */
public class Flink04_WC_Parallelism {
    public static void main(String[] args) throws Exception {

        // TODO 并行度设置
        // 优先级： 算子 > env > 提交参数 > 配置文件

        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // 1.读取数据
        DataStreamSource<String> socketDS = env.socketTextStream("localhost", 9999);

        // 2.处理数据
        // 2.1 扁平化：切分、转成二元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOneTuple = socketDS
                .flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (r, out) -> {
                    // 1.切分
                    String[] words = r.split(" ");
                    for (String word : words) {
                        // 2.转换成二元组（word，1）
                        Tuple2<String, Integer> tuple = Tuple2.of(word, 1);
                        // 3.使用采集器往下游发送数据
                        out.collect(tuple);
                    }
                })
                .returns(new TypeHint<Tuple2<String, Integer>>() {});

        // 2.2 按照 word 分组
        KeyedStream<Tuple2<String, Integer>, Tuple> wordAndOneKS = wordAndOneTuple.keyBy(0);
        // 2.3 聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultDS = wordAndOneKS.sum(1);

        // 3.输出
        resultDS.print();

        // 4. 启动
        env.execute();
    }
}
