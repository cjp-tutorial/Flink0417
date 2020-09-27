package com.atguigu.chapter08;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.util.HashSet;
import java.util.Set;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/9/16 15:29
 */
public class Flink11_Case_UVWithRedis {
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
        SingleOutputStreamOperator<Long> userDS = userBehaviorFilter.map(new MapFunction<UserBehavior, Long>() {
            @Override
            public Long map(UserBehavior value) throws Exception {
                return value.getUserId();
            }
        });

        // 2.3 开窗
        userDS
                .timeWindowAll(Time.hours(1))
                .trigger(
                        new Trigger<Long, TimeWindow>() {
                            //来一条数据，怎么处理？
                            @Override
                            public TriggerResult onElement(Long element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
                                // 来一条数据，触发计算，并且清除不保存
                                return TriggerResult.FIRE_AND_PURGE;
                            }

                            // 到达处理时间，怎么处理？ => 不处理，跳过
                            @Override
                            public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                                return TriggerResult.CONTINUE;
                            }

                            // 到达事件时间，怎么处理？ => 不处理，跳过
                            @Override
                            public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                                return TriggerResult.CONTINUE;
                            }

                            // 清除
                            @Override
                            public void clear(TimeWindow window, TriggerContext ctx) throws Exception {

                            }
                        }
                )
                .process(new UvCountByRedis())
                .print();

        env.execute();
    }

    public static class UvCountByRedis extends ProcessAllWindowFunction<Long, String, TimeWindow> {

        private Jedis jedis;

        @Override
        public void open(Configuration parameters) throws Exception {
            jedis = new Jedis("hadoop102", 6379);
        }

        @Override
        public void process(Context context, Iterable<Long> elements, Collector<String> out) throws Exception {
            MyBloomFilter myBloomFilter = new MyBloomFilter();
            String userId = elements.iterator().next().toString();
            // 得到 bitmap指定位置的索引
            Long offset = myBloomFilter.offset(userId, 61);
            String windowEnd = String.valueOf(context.window().getEnd());
            // 对 key 所储存的字符串值，获取指定偏移量上的位(bit), 1 = true,0 = false。
            // 每个窗口一个 bitmap
            Boolean isExist = jedis.getbit(windowEnd, offset);

            // 如果存在，已统计过，不处理
            // 如果不存在，表示没来过 => 1.更新redis中的uv值 ，+1 2.更新位图为true
            if (!isExist) {
                // 更新redis中的uv值
                String uvCountStr = jedis.hget("uvCount", windowEnd);
                if (uvCountStr == null || "".equals(uvCountStr)) {
                    // 如果是当前窗口第一条数据，给个初始值
                    uvCountStr = "1";
                } else {
                    long uvCount = Long.valueOf(uvCountStr) + 1;
                    uvCountStr = String.valueOf(uvCount);
                }
                // 更新到 redis中
                jedis.hset("uvCount", windowEnd, uvCountStr);
                // 更新位图
                jedis.setbit(windowEnd, offset, true);
            }
            out.collect("数据=" + elements);
        }

        @Override
        public void close() throws Exception {
            jedis.close();
        }
    }

    public static class MyBloomFilter{
        // 容量
        private Integer cap = 1 << 30;
        // hash
        private Long hash = 0L;

        // seed最好给一个质数，能有效避免碰撞
        // 简化版，只用了一个hash函数
        public Long offset(String inputStr, Integer seed) {
            for (int i = 0; i < inputStr.length(); i++) {
                hash = hash * seed + inputStr.charAt(i);
            }
            // 正常应该是 对 cap取模
            // 参考了hashmap的思想，m % (n-1)， 当 n 为 2的整数幂时，可以替换成位运算，这样效率能极大提高
            return hash & (cap - 1);
        }
    }

}
