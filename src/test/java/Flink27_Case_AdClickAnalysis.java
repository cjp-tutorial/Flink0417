import com.atguigu.bean.AdClickLog;
import com.atguigu.bean.HotAdClick;
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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * 每隔5秒，输出最近10分钟内 不同省份点击最多的广告排名（）
 *
 * @author cjp
 * @version 1.0
 * @date 2020/9/23 16:06
 */
public class Flink27_Case_AdClickAnalysis {
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
        // 2.1 按照 统计维度 分组:省份、广告
        KeyedStream<AdClickLog, Tuple2<String, Long>> adClickKS = logDS.keyBy(new KeySelector<AdClickLog, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> getKey(AdClickLog value) throws Exception {
                return Tuple2.of(value.getProvince(), value.getAdId());
            }
        });

        // 2.2 开窗
        SingleOutputStreamOperator<HotAdClick> aggDS = adClickKS
//                .timeWindow(Time.minutes(10), Time.seconds(5))
                .timeWindow(Time.hours(1), Time.minutes(5))
                .aggregate(
                        new SimpleAggFunction<AdClickLog>(),
                        new AdCountResultWithWindowEnd());
        aggDS.print("agg");

        aggDS
                .keyBy(data -> data.getWindowEnd())
                .process(new TopNAdClick(3))
                .print();

        env.execute();
    }

    public static class TopNAdClick extends KeyedProcessFunction<Long, HotAdClick, String> {

        private Integer threshold;
        private ListState<HotAdClick> datas;
        private ValueState<Long> triggerTS;

        public TopNAdClick(Integer threshold) {
            this.threshold = threshold;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            datas = getRuntimeContext().getListState(new ListStateDescriptor<HotAdClick>("datas", HotAdClick.class));
            triggerTS = getRuntimeContext().getState(new ValueStateDescriptor<Long>("triggerTS", Long.class));
        }

        @Override
        public void processElement(HotAdClick value, Context ctx, Collector<String> out) throws Exception {
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
            List<HotAdClick> hotAdClicks = new ArrayList<>();
            for (HotAdClick hotAdClick : datas.get()) {
                System.out.println("datas状态里的数据= " + hotAdClick);
                hotAdClicks.add(hotAdClick);
            }

            System.out.println("list里的数据=" + hotAdClicks.toString());
            // 清空状态，过河拆桥
            datas.clear();
            triggerTS.clear();
            // 排序
            hotAdClicks.sort(new Comparator<HotAdClick>() {
                @Override
                public int compare(HotAdClick o1, HotAdClick o2) {
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


    public static class AdCountResultWithWindowEnd extends ProcessWindowFunction<Long, HotAdClick, Tuple2<String, Long>, TimeWindow> {

        @Override
        public void process(Tuple2<String, Long> key, Context context, Iterable<Long> elements, Collector<HotAdClick> out) throws Exception {
            out.collect(new HotAdClick(key.f0, key.f1, elements.iterator().next(), context.window().getEnd()));
        }
    }

}
