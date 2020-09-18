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

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/9/16 15:29
 */
public class Flink24_Case_PVByProcess {
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

        // TODO 实现 PV的统计
        // 2.处理数据
        // 2.1 过滤出 pv 行为
        SingleOutputStreamOperator<UserBehavior> userBehaviorFilter = userBehaviorDS.filter(data -> "pv".equals(data.getBehavior()));
        // 2.2 按照 统计的维度 分组 ：pv行为
        KeyedStream<UserBehavior, String> userBehaviorKS = userBehaviorFilter.keyBy(data -> data.getBehavior());
        // 2.3 求和 => 实现 计数 的功能，没有count这种聚合算子
        // 一般找不到现成的算子，那就调用底层的 process
        SingleOutputStreamOperator<Long> resultDS = userBehaviorKS.process(
                new KeyedProcessFunction<String, UserBehavior, Long>() {
                    // 定义一个变量，来统计条数
                    private Long pvCount = 0L;

                    /**
                     * 来一条处理一条
                     * @param value
                     * @param ctx
                     * @param out
                     * @throws Exception
                     */
                    @Override
                    public void processElement(UserBehavior value, Context ctx, Collector<Long> out) throws Exception {
                        // 来一条，count + 1
                        pvCount++;
                        // 采集器往下游发送统计结果
                        out.collect(pvCount);
                    }
                }
        );

        resultDS.print("pv by process");


        env.execute();
    }

}
