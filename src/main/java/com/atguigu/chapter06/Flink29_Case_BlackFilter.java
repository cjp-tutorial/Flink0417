package com.atguigu.chapter06;

import com.atguigu.bean.AdClickLog;
import com.atguigu.bean.HotAdClickByUser;
import com.atguigu.bean.SimpleAggFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * 每隔5秒，输出最近10分钟内 不同用户点击最多的广告排名（）
 *
 * @author cjp
 * @version 1.0
 * @date 2020/9/23 16:06
 */
public class Flink29_Case_BlackFilter {
    public static void main(String[] args) throws Exception {
        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 1.读取数据、转换
        SingleOutputStreamOperator<AdClickLog> logDS = env
                .readTextFile("input/AdClickLog.csv")
                .map(new MapFunction<String, AdClickLog>() {
                    @Override
                    public AdClickLog map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new AdClickLog(
                                Long.valueOf(datas[0]),
                                Long.valueOf(datas[1]),
                                datas[2],
                                datas[3],
                                Long.valueOf(datas[4])
                        );
                    }
                })
                .assignTimestampsAndWatermarks(
                        new AscendingTimestampExtractor<AdClickLog>() {
                            @Override
                            public long extractAscendingTimestamp(AdClickLog element) {
                                return element.getTimestamp() * 1000L;
                            }
                        }
                );

        // 2.处理数据
        // TODO 黑名单过滤 ：每天 100次
        // 过滤：不改变数据的格式、类型， 只是对不符合要求的数据过滤掉、丢弃、告警
        // 2.1 按照 统计维度 分组:用户、广告
        KeyedStream<AdClickLog, Tuple2<Long, Long>> adClickKS = logDS.keyBy(new KeySelector<AdClickLog, Tuple2<Long, Long>>() {
            @Override
            public Tuple2<Long, Long> getKey(AdClickLog value) throws Exception {
                return Tuple2.of(value.getUserId(), value.getAdId());
            }
        });
        // 2.2 过滤
        SingleOutputStreamOperator<AdClickLog> filterDS = adClickKS.process(new BlackFilter());

        OutputTag<String> alarm = new OutputTag<String>("blacklist") {
        };
        filterDS.getSideOutput(alarm).print("black");

        // 2.3 接着往下做分析 =》 需要重新分组，因为过滤完，没有分组信息了
        filterDS
                .keyBy(new KeySelector<AdClickLog, Tuple2<Long, Long>>() {
                    @Override
                    public Tuple2<Long, Long> getKey(AdClickLog value) throws Exception {
                        return Tuple2.of(value.getUserId(), value.getAdId());
                    }
                })
                .timeWindow(Time.hours(1),Time.minutes(5))
                .aggregate(new SimpleAggFunction<AdClickLog>(), new AdCountResultWithWindowEnd())
                .keyBy(data -> data.getWindowEnd())
                .process(new TopNAdClick(3))
                .print("top3");


        env.execute();
    }

    public static class BlackFilter extends KeyedProcessFunction<Tuple2<Long, Long>, AdClickLog, AdClickLog> {

        private ValueState<Integer> clickCount;
        private ValueState<Boolean> alarmFlag;
        private Long triggerTs = 0L;
        OutputTag<String> alarm = new OutputTag<String>("blacklist") {
        };

        @Override
        public void open(Configuration parameters) throws Exception {
            clickCount = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("clickCount", Integer.class, 0));
            alarmFlag = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("alarmFlag", Boolean.class, false));
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
        public void processElement(AdClickLog value, Context ctx, Collector<AdClickLog> out) throws Exception {
            // 隔天0点清空状态（点击量）
            // 当前时间、怎么获取今天？ =》 第一条数据来的时候，获取隔天0点的时间

            if (triggerTs == 0) {
                // 获取隔天 0点
                // 1.获取 今天的0点 => 向下取整 =》 1970年开始到 今天 有 过少 天
                // 2020/09/24 10:12 =》 2020/09/24 00:00 距离 1970年 经过了 多少天
                long currentDays = ctx.timestamp() / (24 * 60 * 60 * 1000L);
                // 2.隔天 = 天数 + 1
                // 2020/09/25 00:00 距离 1970年 经过了 多少天
                long nextDays = currentDays + 1;
                // 3.转成 时间戳
                // 2020/09/25 00:00 对应的 时间戳
                long nextDayTs = nextDays * (24 * 60 * 60 * 1000L);
                // 4.注册定时器
                ctx.timerService().registerEventTimeTimer(nextDayTs);
                triggerTs = nextDayTs;
            }


            Integer currentClickCount = clickCount.value();
            if (currentClickCount >= 100) {
                // 达到阈值，侧输出流告警，第一次才进行告警
                if (!alarmFlag.value()) {
                    ctx.output(alarm, "用户" + value.getUserId() + "点击了" + currentClickCount + "次" + value.getAdId() + "广告，可能为恶意刷点击量！！！");
                    alarmFlag.update(true);
                }
            } else {
                clickCount.update(currentClickCount + 1);
                // 没有达到阈值，正常往下游传递数据
                out.collect(value);
            }
        }

        /**
         * 定时器触发：到了隔天0点，需要清空统计值
         *
         * @param timestamp
         * @param ctx
         * @param out
         * @throws Exception
         */
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<AdClickLog> out) throws Exception {
            System.out.println(timestamp + "触发器触发，清空clickCount");
            clickCount.clear();
            System.out.println(timestamp + "的clickcount=" + clickCount.value());
            triggerTs = 0L;
            alarmFlag.clear();
        }
    }


    public static class TopNAdClick extends KeyedProcessFunction<Long, HotAdClickByUser, String> {

        private Integer threshold;
        private ListState<HotAdClickByUser> datas;
        private ValueState<Long> triggerTS;

        public TopNAdClick(Integer threshold) {
            this.threshold = threshold;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            datas = getRuntimeContext().getListState(new ListStateDescriptor<HotAdClickByUser>("datas", HotAdClickByUser.class));
            triggerTS = getRuntimeContext().getState(new ValueStateDescriptor<Long>("triggerTS", Long.class));
        }

        @Override
        public void processElement(HotAdClickByUser value, Context ctx, Collector<String> out) throws Exception {
            // 存数据
            datas.add(value);
            // 模拟窗口触发，注册定时器
            if (triggerTS.value() == null) {
                ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 10);
                triggerTS.update(value.getWindowEnd() + 10);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            //
            List<HotAdClickByUser> hotAdClicks = new ArrayList<>();
            for (HotAdClickByUser hotAdClick : datas.get()) {
                hotAdClicks.add(hotAdClick);
            }
            // 清空状态，过河拆桥
            datas.clear();
            triggerTS.clear();
            // 排序
            hotAdClicks.sort(new Comparator<HotAdClickByUser>() {
                @Override
                public int compare(HotAdClickByUser o1, HotAdClickByUser o2) {
                    return o2.getClickCount().intValue() - o1.getClickCount().intValue();
                }
            });
            // 取前 N 个
            StringBuilder resultStr = new StringBuilder();
            resultStr.append("窗口结束时间:" + (timestamp - 10) + "\n")
                    .append("---------------------------------------------------\n");

            // 加一个判断逻辑： threshold 是否超过 list的大小
            threshold = threshold > hotAdClicks.size() ? hotAdClicks.size() : threshold;
            for (int i = 0; i < threshold; i++) {
                resultStr.append(hotAdClicks.get(i) + "\n");
            }
            resultStr.append("--------------------------------------------------\n\n");

            out.collect(resultStr.toString());
        }
    }


    public static class AdCountResultWithWindowEnd extends ProcessWindowFunction<Long, HotAdClickByUser, Tuple2<Long, Long>, TimeWindow> {

        @Override
        public void process(Tuple2<Long, Long> key, Context context, Iterable<Long> elements, Collector<HotAdClickByUser> out) throws Exception {
            out.collect(new HotAdClickByUser(key.f0, key.f1, elements.iterator().next(), context.window().getEnd()));
        }
    }

}
