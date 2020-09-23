package com.atguigu.chapter06;

import com.atguigu.bean.HotItemCountWithWindowEnd;
import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
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

import java.util.*;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/9/23 10:13
 */
public class Flink25_Case_HotItemsAnalysis {
    public static void main(String[] args) throws Exception {
        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 1.读取数据
        SingleOutputStreamOperator<UserBehavior> userBehaviorDS = env.readTextFile("input/UserBehavior.csv")
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new UserBehavior(
                                Long.valueOf(datas[0]),
                                Long.valueOf(datas[1]),
                                Integer.valueOf(datas[2]),
                                datas[3],
                                Long.valueOf(datas[4]));
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

        // 2.处理数据
        // 2.1 过滤出 pv 行为
        SingleOutputStreamOperator<UserBehavior> userBehaviorFilter = userBehaviorDS.filter(data -> "pv".equals(data.getBehavior()));
        // 2.2 按照 统计维度 分组 ： 商品
        KeyedStream<UserBehavior, Long> userBehaiorKS = userBehaviorFilter.keyBy(data -> data.getItemId());
        // 2.3 开窗：每5分钟输出最近一小时 => 滑动窗口，长度1小时，步长5分钟
        WindowedStream<UserBehavior, Long, TimeWindow> userBehaviorWS = userBehaiorKS.timeWindow(Time.hours(1), Time.seconds(5));
        // TODO 2.4 求和统计 => 每个商品被点击多少次
        // sum? =》需要转成元组（xxx,1），按照 1 求和 =》求完和怎么排序？
        // reduce？ => 可以得到统计结果,输入和输出的类型要一致 => 排序？
        // aggregate => 可以得到统计结果 => 排序？
        // process => 可以得到统计结果 => 是全窗口函数，会存数据，有oom风险

        // => 调用聚合操作之后，每个窗口的输出都混到一起，没有窗口的概念了
        // => 需求是每小时的 topN，也就是说，排序是本窗口的数据进行排序，不同窗口混到一起怎么办？

        // agggate用法介绍：传两个参数
        //      第一个参数：预聚合函数 => 增量聚合
        //      第二个参数：全窗口函数
        //      预聚合的结果，会作为 全窗口函数的 输入 => 数据量变小了
        //      1001
        //      1001
        //      1001
        //      1001
        //      1002
        //      1002
        //      传递给全窗口的就是 4,2 两条数据，但是 分组的标签 是能获取到的
        //      => 相当于，每个商品一条统计结果，也就是有几种商品，就有几条数据
        //      => 按照咱们的规模，最多也就几十万上下，没压力
        //      第二个参数：全窗口函数，目的是给统计结果，打上窗口的标签，因为聚合之后窗口就没了
        SingleOutputStreamOperator<HotItemCountWithWindowEnd> aggDS = userBehaviorWS.aggregate(new AggCount(), new CountResultWithWindowEnd());

        // 2.5 按照 窗口结束时间 分组 => 让属于同一个窗口的统计结果，到一起去，后续进行排序
        KeyedStream<HotItemCountWithWindowEnd, Long> aggKS = aggDS.keyBy(data -> data.getWindowEnd());

        // 2.6 排序
        SingleOutputStreamOperator<String> topN = aggKS.process(new TopNItems(3));

        topN.print();


        env.execute();
    }


    public static class TopNItems extends KeyedProcessFunction<Long, HotItemCountWithWindowEnd, String> {

        //定义一个属性，用来决定，要 top 几
        private Integer threshold;

        private ListState<HotItemCountWithWindowEnd> dataList;
        private ValueState<Long> triggerTs;

        public TopNItems(Integer threshold) {
            this.threshold = threshold;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            dataList = getRuntimeContext().getListState(new ListStateDescriptor<HotItemCountWithWindowEnd>("dataList", HotItemCountWithWindowEnd.class));
            triggerTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>("triggerTs", Long.class));
        }

        /**
         * 来一条处理一条
         *
         * @param value
         * @param ctx
         * @param out
         * @throws Exception
         */
        @Override
        public void processElement(HotItemCountWithWindowEnd value, Context ctx, Collector<String> out) throws Exception {
            // 排序，需要 属于同一窗口的 数据 都到齐 => 来一条，存一条
            dataList.add(value);
            // 存到什么时候为止？ => 什么时候是到齐？ => 什么时候排序？
            // => 模拟窗口的触发， 注册一个 窗口结束时间 的定时器
            if (triggerTs.value() == null) {
                ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 100L);
                triggerTs.update(value.getWindowEnd()+ 100);
            }
        }

        /**
         * 定时器触发：触发说明 属于同一个窗口的 统计结果 到齐了 => 排序，取 前N个
         *
         * @param timestamp
         * @param ctx
         * @param out
         * @throws Exception
         */
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            //
            Iterable<HotItemCountWithWindowEnd> datas = dataList.get();
            //创建一个List，把数据 复制过来
            List<HotItemCountWithWindowEnd> list = new ArrayList<>();
            for (HotItemCountWithWindowEnd data : datas) {
                list.add(data);
            }
            // 使用完毕，状态已经没用了，及时清除，释放空间
            dataList.clear();
            triggerTs.clear();

            // 排序方式1
//            list.sort(new Comparator<HotItemCountWithWindowEnd>() {
//                @Override
//                public int compare(HotItemCountWithWindowEnd o1, HotItemCountWithWindowEnd o2) {
//                    // 降序 => 后 减 前
//                    return o2.getItemCount().intValue() - o1.getItemCount().intValue();
//                }
//            });

            //排序方式2
            Collections.sort(list);

            StringBuilder resultStr = new StringBuilder();
            resultStr
                    .append("窗口结束时间:" + timestamp + "\n")
                    .append("------------------------------------------------------\n");

            for (int i = 0; i < threshold; i++) {
                resultStr.append(list.get(i) + "\n");
            }
            resultStr.append("------------------------------------------------------\n\n");


            out.collect(resultStr.toString());
        }
    }


    /**
     * 全窗口函数：目的是 给 统计结果 打上 窗口结束时间的标签，用于区分 统计结果 属于哪个窗口
     */
    public static class CountResultWithWindowEnd extends ProcessWindowFunction<Long, HotItemCountWithWindowEnd, Long, TimeWindow> {

        /**
         * 一次处理一个组的所有数据：每个组（商品）只有一条聚合结果了
         * 给聚合的结果，打上窗口的标签，标签是：窗口的结束时间戳
         *
         * @param key
         * @param context
         * @param elements
         * @param out
         * @throws Exception
         */
        @Override
        public void process(Long key, Context context, Iterable<Long> elements, Collector<HotItemCountWithWindowEnd> out) throws Exception {
            out.collect(new HotItemCountWithWindowEnd(key, elements.iterator().next(), context.window().getEnd()));
        }
    }

    /**
     * 预聚合函数：输出 会传递给 全窗口函数
     */
    public static class AggCount implements AggregateFunction<UserBehavior, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
            return accumulator + 1L;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }
}
