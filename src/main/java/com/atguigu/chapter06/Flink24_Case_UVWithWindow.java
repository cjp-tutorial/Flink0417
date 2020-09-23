package com.atguigu.chapter06;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/9/16 15:29
 */
public class Flink24_Case_UVWithWindow {
    public static void main(String[] args) throws Exception {

        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 1.从文件读取数据、转换成 bean对象
        SingleOutputStreamOperator<UserBehavior> userBehaviorDS = env
                .readTextFile("input/UserBehavior.csv")
                .map(new MapFunction<String, UserBehavior>() {

                    @Override
                    public UserBehavior map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new UserBehavior(
                                Long.valueOf(datas[0]),
                                Long.valueOf(datas[1]),
                                Integer.valueOf(datas[2]),
                                datas[3],
                                Long.valueOf(datas[4])
                        );
                    }
                })
                .assignTimestampsAndWatermarks(
                        new AscendingTimestampExtractor<UserBehavior>() {
                            @Override
                            public long extractAscendingTimestamp(UserBehavior element) {
                                return element.getTimestamp() * 1000L;
                            }
                        }
                );

        // TODO 实现 UV的统计 ：对 userId进行去重，统计

        // 2.处理数据
        // 2.1 过滤出 pv 行为 => UV 就是 PV的去重，所以行为还是 pv
        SingleOutputStreamOperator<UserBehavior> userBehaviorFilter = userBehaviorDS.filter(data -> "pv".equals(data.getBehavior()));
        // 2.2 转换成二元组 ("uv",userId)
        // => 第一个给 uv，是为了分组，分组是为了调用 sum或 process等方法
        // => 第二个给userId，是为了将 userId存到 Set里
        // => 这里我们，只需要userId,其他什么商品、品类这些都不需要
        SingleOutputStreamOperator<Tuple2<String, Long>> uvTuple2 = userBehaviorFilter.map(new MapFunction<UserBehavior, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(UserBehavior value) throws Exception {
                return Tuple2.of("uv", value.getUserId());
            }
        });

        // 2.3 按照 uv 分组
        KeyedStream<Tuple2<String, Long>, String> uvKS = uvTuple2.keyBy(data -> data.f0);

        // TODO 开窗
        WindowedStream<Tuple2<String, Long>, String, TimeWindow> uvWS = uvKS.timeWindow(Time.hours(1));

        // 2.4 使用 process 处理
        SingleOutputStreamOperator<Long> uvDS = uvWS.process(
                new ProcessWindowFunction<Tuple2<String, Long>, Long, String, TimeWindow>() {
                    private Set<Long> uvCount = new HashSet<>();

                    @Override
                    public void process(String s, Context context, Iterable<Tuple2<String, Long>> elements, Collector<Long> out) throws Exception {
                        for (Tuple2<String, Long> element : elements) {
                            uvCount.add(element.f1);
                        }
                        out.collect(Long.valueOf(uvCount.size()));
                        uvCount.clear();
                    }
                }
        );


        uvDS.print("uv");

        env.execute();
    }

}
