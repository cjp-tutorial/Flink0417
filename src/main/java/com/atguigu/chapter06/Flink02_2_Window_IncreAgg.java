package com.atguigu.chapter06;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/9/19 11:22
 */
public class Flink02_2_Window_IncreAgg {
    public static void main(String[] args) throws Exception {
        // 0 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.
        DataStreamSource<String> socketDS = env.socketTextStream("localhost", 9999);

        KeyedStream<Tuple2<String, Integer>, String> dataKS = socketDS
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        return Tuple2.of(value, 1);
                    }
                })
                .keyBy(r -> r.f0);


        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> dataWS = dataKS
                .timeWindow(Time.seconds(5));

        //TODO 增量聚合
        // 来一条处理一条，窗口关闭的时候，才会输出一次结果

        dataWS
//                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
//                    @Override
//                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
//                        System.out.println(value1 + "<---->" + value2);
//                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
//                    }
//                })
                .aggregate(
                        new AggregateFunction<Tuple2<String,Integer>, Integer, Integer>() {

                            /**
                             * 创建累加器 =》 初始化  => 这边做的是累加，那么给个初始值 0
                             * @return
                             */
                            @Override
                            public Integer createAccumulator() {
                                return 0;
                            }

                            /**
                             * 累加操作 => 每来一条数据，如何进行累加
                             * @param value
                             * @param accumulator
                             * @return
                             */
                            @Override
                            public Integer add(Tuple2<String, Integer> value, Integer accumulator) {
                                System.out.println("add...");
                                return accumulator + 1;
                            }

                            /**
                             * 获取结果
                             * @param accumulator
                             * @return
                             */
                            @Override
                            public Integer getResult(Integer accumulator) {
                                System.out.println("get result...");
                                return accumulator;
                            }

                            /**
                             * 会话窗口 才会调用：合并累加器的结果
                             * @param a
                             * @param b
                             * @return
                             */
                            @Override
                            public Integer merge(Integer a, Integer b) {
                                return a + b;
                            }
                        }
                )
                .print();

        env.execute();
    }
}
