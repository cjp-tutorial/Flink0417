package com.atguigu.chapter06;

import com.atguigu.bean.OrderEvent;
import com.atguigu.bean.TxEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.Map;

/**
 * 订单超时监测
 *
 * @author cjp
 * @version 1.0
 * @date 2020/9/18 16:32
 */
public class Flink31_Case_OrderTimeoutDetect {
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


        //TODO 存在的问题
        // 1.如果订单还有 修改、取消 等其他状态，代码需要大幅度的增加

        // 2.处理数据：订单超时监控
        // 2.1 按照 统计维度 分组：订单
        KeyedStream<OrderEvent, Long> orderKS = orderDS.keyBy(order -> order.getOrderId());
        // 2.2 超时分析
        SingleOutputStreamOperator<String> resultDS = orderKS.process(new OrderTimeoutDetect());

        resultDS.print("result");

        OutputTag timeoutTag = new OutputTag<String>("timeout") {
        };
        resultDS.getSideOutput(timeoutTag).print("timeout");

        env.execute();
    }

    public static class OrderTimeoutDetect extends KeyedProcessFunction<Long, OrderEvent, String> {
        private ValueState<OrderEvent> payState;
        private ValueState<OrderEvent> createState;
        private ValueState<Long> timeoutTs;
        OutputTag timeoutTag = new OutputTag<String>("timeout") {
        };

        @Override
        public void open(Configuration parameters) throws Exception {
            payState = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("payState", OrderEvent.class));
            createState = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("createState", OrderEvent.class));
            timeoutTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timeoutTs", Long.class));
        }

        @Override
        public void processElement(OrderEvent value, Context ctx, Collector<String> out) throws Exception {
            // 还要考虑：只来一个数据的情况
            if (timeoutTs.value() == null) {
                // 这里不用考虑来的是create还是pay，统一都等15分钟
                ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 15 * 60 * 1000L);
                timeoutTs.update(ctx.timestamp() + 15 * 60 * 1000L);
            } else {
                // 说明另一条数据来了，删除定时器
                ctx.timerService().deleteEventTimeTimer(timeoutTs.value());
                timeoutTs.clear();
            }

            // 数据可能是乱序的，对同一个订单而言，可能是 pay的数据先到
            // 判断一下当前来的是什么数据
            if ("create".equals(value.getEventType())) {
                // 1.说明当前数据是 create => 判断 pay是否来过
                if (payState.value() == null) {
                    // 1.1 pay没来过 => 把 create存起来
                    createState.update(value);
                } else {
                    // 1.2 pay来过 =》 判断是否超时
                    if (payState.value().getEventTime() - value.getEventTime() > 15 * 60) {
                        // 1.2.1 超时 => 告警
                        ctx.output(timeoutTag, "订单" + value.getOrderId() + "支付成功，但是超时，系统可能存在漏洞，请及时修复！！！");
                    } else {
                        // 1.2.2 没超时
                        out.collect("订单" + value.getOrderId() + "支付成功！");
                    }
                    // 使用完，清空
                    payState.clear();
                }
            } else {
                // 2.说明当前数据是 pay => 判断 create是否来过
                if (createState.value() == null) {
                    // 2.1 说明 create没来过 => 把当前的 pay存起来
                    payState.update(value);
                } else {
                    // 2.2 说明 create来过 => 判断是否超时
                    if (value.getEventTime() - createState.value().getEventTime() > 15 * 60) {
                        // 2.2.1 超时
                        ctx.output(timeoutTag, "订单" + value.getOrderId() + "支付成功，但是超时，系统可能存在漏洞，请及时修复！！！");
                    } else {
                        // 2.2.2 没超时
                        out.collect("订单" + value.getOrderId() + "支付成功！");
                    }
                    // 使用完，清空
                    createState.clear();
                }
            }
        }

        /**
         * 定时器触发：说明另一条数据没来
         * @param timestamp
         * @param ctx
         * @param out
         * @throws Exception
         */
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 判断一下是谁没来
            // 如果是create没来 => 说明pay来过，说明payState不为空
            if (payState.value() != null){
                // create没来 => 异常情况，告警
                ctx.output(timeoutTag, "订单"+payState.value().getOrderId()+"有支付数据，但下单数据丢失，系统存在异常！！！");
                // 清空
                payState.clear();
            }
            // 如果是pay没来 => 说明create来过，说明createState不为空
            if (createState.value() != null){
                // pay没来 => 没支付
                ctx.output(timeoutTag,"订单"+createState.value().getOrderId()+"支付超时！！！" );
                // 清空
                createState.clear();
            }
            timeoutTs.clear();
        }
    }


}
