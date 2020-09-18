package com.atguigu.chapter05;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
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
public class Flink26_Case_UV {
    public static void main(String[] args) throws Exception {

        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

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
                });

        // TODO 实现 UV的统计 ：对 userId进行去重，统计
        // => userId存在一个Set中
        // => 算出Set里的元素个数，就是UV值

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
        
        // 2.4 使用 process 处理
        SingleOutputStreamOperator<Integer> uvDS = uvKS.process(
                new KeyedProcessFunction<String, Tuple2<String, Long>, Integer>() {
                    // 定义一个Set，用来去重并存放 userId
                    private Set<Long> uvSet = new HashSet<>();

                    /**
                     * 来一条数据处理一条
                     * @param value
                     * @param ctx
                     * @param out
                     * @throws Exception
                     */
                    @Override
                    public void processElement(Tuple2<String, Long> value, Context ctx, Collector<Integer> out) throws Exception {
                        // 来一条数据，就把 userId存到 Set中
                        uvSet.add(value.f1);
                        // 通过采集器，往下游发送 uv值 => Set中元素的个数，就是 UV值
                        out.collect(uvSet.size());
                    }
                }
        );

        uvDS.print("uv");


        env.execute();
    }

}
