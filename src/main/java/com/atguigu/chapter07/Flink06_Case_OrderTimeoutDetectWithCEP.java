package com.atguigu.chapter07;

import com.atguigu.bean.OrderEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
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
public class Flink06_Case_OrderTimeoutDetectWithCEP {
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
        // TODO select 可以传三个参数
        // 1.第一个参数： 侧输出流的 OutputTag 对象
        // 2.超时数据的处理：对超时数据进行 xx 处理，放入 侧输出流
        // 3.匹配上的数据的处理： 对正常匹配上的数据进行 xx 处理，放入 主流
        // CEP只能处理正常情况，比如 系统存在异常、漏洞这些情况 没法发现
        OutputTag<String> timeoutTag = new OutputTag<String>("timeout") {
        };
        SingleOutputStreamOperator<String> resultDS = orderPS.select(timeoutTag, new TimeOutFunction(), new SelectFunction());

//        resultDS.print("result");
        resultDS.getSideOutput(timeoutTag).print("timeout");

        env.execute();
    }

    public static class TimeOutFunction implements PatternTimeoutFunction<OrderEvent, String> {

        @Override
        public String timeout(Map<String, List<OrderEvent>> map, long timeoutTimestamp) throws Exception {
            String keys = map.keySet().toString();
            String values = map.values().toString();
            StringBuilder timeoutStr = new StringBuilder();
            timeoutStr
                    .append("======================================================\n")
                    .append("map里所有的key=" + keys + "\n")
                    .append("map里所有的value=" + values)
                    .append("======================================================\n\n\n\n");
            return timeoutStr.toString();
        }
    }


    public static class SelectFunction implements PatternSelectFunction<OrderEvent, String> {

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
    }

}
