package com.atguigu.chapter07;

import com.atguigu.bean.OrderEvent;
import com.atguigu.bean.TxEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * 实时对账 监控
 *
 * @author cjp
 * @version 1.0
 * @date 2020/9/18 16:32
 */
public class Flink07_Case_OrderTxDetectWithIntervalJoin {
    public static void main(String[] args) throws Exception {
        // 0 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
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
                        new BoundedOutOfOrdernessTimestampExtractor<OrderEvent>(Time.seconds(10)) {
                            @Override
                            public long extractTimestamp(OrderEvent element) {
                                return element.getEventTime() * 1000L;
                            }
                        }
                );

        SingleOutputStreamOperator<TxEvent> txDS = env
                .readTextFile("input/ReceiptLog.csv")
                .map(new MapFunction<String, TxEvent>() {
                    @Override
                    public TxEvent map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new TxEvent(
                                datas[0],
                                datas[1],
                                Long.valueOf(datas[2])
                        );
                    }
                })
                .assignTimestampsAndWatermarks(
                        new AscendingTimestampExtractor<TxEvent>() {
                            @Override
                            public long extractAscendingTimestamp(TxEvent element) {
                                return element.getEventTime() * 1000L;
                            }
                        }
                );


        // 2.处理数据：实时对账 监控
        //TODO Interval Join 实现
        // 必须是 事件时间 语义下
        // 两条流要是 KeyedStream，也就是要先 keyby
        // 2.1 按照 匹配的数据 分组：交易码
        KeyedStream<OrderEvent, String> orderKS = orderDS.keyBy(order -> order.getTxId());
        KeyedStream<TxEvent, String> txKS = txDS.keyBy(tx -> tx.getTxId());

        // 2.2 使用 interval join 关联
        SingleOutputStreamOperator<String> resultDS = orderKS
                .intervalJoin(txKS)
                .between(Time.seconds(-15), Time.seconds(15))
                .process(new JoinFunction());

        resultDS.print();


        env.execute();
    }

    public static class JoinFunction extends ProcessJoinFunction<OrderEvent, TxEvent, String> {

        @Override
        public void processElement(OrderEvent left, TxEvent right, Context ctx, Collector<String> out) throws Exception {
            // 匹配 txid
            if (left.getTxId().equals(right.getTxId())) {
                out.collect("订单" + left.getOrderId() + "对账成功");
            }
        }
    }

}
