package com.atguigu.chapter05;

import com.atguigu.bean.MarketingUserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * 不同渠道不同行为 的统计
 *
 * @author cjp
 * @version 1.0
 * @date 2020/9/18 16:32
 */
public class Flink27_Case_APPMarketingAnalysis {
    public static void main(String[] args) throws Exception {
        // 0 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.读取数据、转换成 bean对象
        DataStreamSource<MarketingUserBehavior> appDS = env.addSource(new AppSource());

        // 2.处理数据：不同渠道不同行为 的统计
        // 2.1 按照 统计的维度 分组 ： 渠道、行为 ，多个维度可以拼接在一起
        // 2.1.1 转换成 （渠道_行为，1）二元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> channelAndBehaviorTuple2 = appDS.map(new MapFunction<MarketingUserBehavior, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(MarketingUserBehavior value) throws Exception {
                return Tuple2.of(value.getChannel() + "_" + value.getBehavior(), 1);
            }
        });
        // 2.1.2 按照 渠道_行为 分组
        KeyedStream<Tuple2<String, Integer>, String> channelAndBehaviorKS = channelAndBehaviorTuple2.keyBy(data -> data.f0);

        // 2.2 求和
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultDS = channelAndBehaviorKS.sum(1);

        // 3. 输出：打印
        resultDS.print("app marketing analysis by channel and behavior");


        env.execute();
    }


    public static class AppSource implements SourceFunction<MarketingUserBehavior> {

        private boolean flag = true;
        private List<String> behaviorList = Arrays.asList("DOWNLOAD", "INSTALL", "UPDATE", "UNINSTALL");
        private List<String> channelList = Arrays.asList("XIAOMI", "HUAWEI", "OPPO", "VIVO");

        @Override
        public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {
            while (flag) {
                Random random = new Random();
                ctx.collect(
                        new MarketingUserBehavior(
                                Long.valueOf(random.nextInt(10)),
                                behaviorList.get(random.nextInt(behaviorList.size())),
                                channelList.get(random.nextInt(channelList.size())),
                                System.currentTimeMillis()
                        )
                );
                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            flag = false;
        }
    }
}
