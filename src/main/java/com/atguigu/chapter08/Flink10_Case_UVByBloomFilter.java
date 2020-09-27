package com.atguigu.chapter08;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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
public class Flink10_Case_UVByBloomFilter {
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
        SingleOutputStreamOperator<UserBehavior> userBehaviorFilter = userBehaviorDS.filter(data -> "pv".equals(data.getBehavior()));
        // 2.2 转换成二元组 ("uv",userId)
        SingleOutputStreamOperator<Tuple2<String, Long>> uvTuple2 = userBehaviorFilter.map(new MapFunction<UserBehavior, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(UserBehavior value) throws Exception {
                return Tuple2.of("uv", value.getUserId());
            }
        });

        // 2.3 按照 uv 分组
        KeyedStream<Tuple2<String, Long>, String> uvKS = uvTuple2.keyBy(data -> data.f0);

        // 2.4 开窗
        WindowedStream<Tuple2<String, Long>, String, TimeWindow> uvWS = uvKS.timeWindow(Time.hours(1));

        // TODO 2.5 使用Flink官方提供的布隆过滤器实现 UV 去重
        SingleOutputStreamOperator<String> uvDS = uvWS.aggregate(new AggWithBloomFilter(), new MyProcessWindowFunction());

        uvDS.print();


        env.execute();
    }

    public static class MyProcessWindowFunction extends ProcessWindowFunction<Long, String, String, TimeWindow> {

        @Override
        public void process(String s, Context context, Iterable<Long> elements, Collector<String> out) throws Exception {
            long start = context.window().getStart();
            long end = context.window().getEnd();
            out.collect("窗口[" + start + "," + end + ")的uv值为:" + elements.iterator().next());
        }
    }

    /**
     * 预聚合函数：使用布隆过滤器去重统计
     */
    public static class AggWithBloomFilter implements AggregateFunction<Tuple2<String, Long>, Tuple2<Long, BloomFilter<Long>>, Long> {

        @Override
        public Tuple2<Long, BloomFilter<Long>> createAccumulator() {
            //   funnel：数据类型(一般是调用Funnels工具类中的)
            //   expectedInsertions：期望插入的值的个数
            //   fpp 错误率(默认值为0.03)
            //   错误率越大，所需空间和时间越小，错误率越小，所需空间和时间约大
//            return Tuple2.of(0L, BloomFilter.create(Funnels.longFunnel(), 1000000000, 0.01));
            return Tuple2.of(0L, BloomFilter.create(Funnels.longFunnel(), 1000000, 0.01));
        }

        @Override
        public Tuple2<Long, BloomFilter<Long>> add(Tuple2<String, Long> value, Tuple2<Long, BloomFilter<Long>> accumulator) {
            Long userId = value.f1;
            Long uvCount = accumulator.f0;
            BloomFilter<Long> bloomFilter = accumulator.f1;
            // 判断当前 userId 在布隆过滤器中是否存在
            if (!bloomFilter.mightContain(userId)) {
                // 不存在的情况，统计值加1
                uvCount++;
                bloomFilter.put(userId);
            }
            return Tuple2.of(uvCount, bloomFilter);
        }

        @Override
        public Long getResult(Tuple2<Long, BloomFilter<Long>> accumulator) {
            return accumulator.f0;
        }

        @Override
        public Tuple2<Long, BloomFilter<Long>> merge(Tuple2<Long, BloomFilter<Long>> a, Tuple2<Long, BloomFilter<Long>> b) {
            return null;
        }
    }

}
