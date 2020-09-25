package com.atguigu.chapter07;

import com.atguigu.bean.OrderEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * 订单超时监测
 *
 * @author cjp
 * @version 1.0
 * @date 2020/9/18 16:32
 */
public class Flink05_Case_OrderTimeoutDetectWithCEP {
    public static void main(String[] args) throws Exception {
        // 0 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 1.读取数据，转成bean对象
        SingleOutputStreamOperator<OrderEvent> orderDS = env
                .readTextFile("input/OrderLog.csv")
                .map(new MapFunction<String, OrderEvent>() {
                    @Override
                    public OrderEvent map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new OrderEvent(
                                Long.valueOf(datas[0]),
                                datas[1],
                                datas[2],
                                Long.valueOf(datas[3])
                        );
                    }
                })
                .assignTimestampsAndWatermarks(
                        new AscendingTimestampExtractor<OrderEvent>() {
                            @Override
                            public long extractAscendingTimestamp(OrderEvent element) {
                                return element.getEventTime() * 1000L;
                            }
                        }
                );

        // 2.处理数据：订单超时监控
        // 2.1 按照 统计维度 分组：订单
        KeyedStream<OrderEvent, Long> orderKS = orderDS.keyBy(order -> order.getOrderId());
        //TODO 2.2 CEP超时分析
        // 1. 定义规则
        Pattern<OrderEvent, OrderEvent> pattern = Pattern
                .<OrderEvent>begin("create")
                .where(new IterativeCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value, Context<OrderEvent> ctx) throws Exception {
                        return "create".equals(value.getEventType());
                    }
                })
                .followedBy("pay")
                .where(new IterativeCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value, Context<OrderEvent> ctx) throws Exception {
                        return "pay".equals(value.getEventType());
                    }
                })
                .within(Time.minutes(15));

        // 2. 使用规则
        PatternStream<OrderEvent> orderPS = CEP.pattern(orderKS, pattern);

        // 3. 获取匹配结果
        SingleOutputStreamOperator<String> resultDS = orderPS.select(new PatternSelectFunction<OrderEvent, String>() {
            @Override
            public String select(Map<String, List<OrderEvent>> map) throws Exception {
                OrderEvent createEvent = map.get("create").iterator().next();
                OrderEvent payEvent = map.get("pay").iterator().next();
                StringBuilder resultStr = new StringBuilder();
                resultStr
                        .append("订单ID:" + createEvent.getOrderId() + "\n")
                        .append("下单时间:" + createEvent.getEventTime() + "\n")
                        .append("支付时间:" + payEvent.getEventTime() + "\n")
                        .append("耗时:" + (payEvent.getEventTime() - createEvent.getEventTime()) + "秒\n")
                        .append("=======================================================================\n\n");
                return resultStr.toString();
            }
        });

        resultDS.print();

        env.execute();
    }
}
