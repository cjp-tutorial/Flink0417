package com.atguigu.chapter05;

import com.atguigu.bean.AdClickLog;
import com.atguigu.bean.OrderEvent;
import com.atguigu.bean.TxEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
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
public class Flink30_Case_OrderTxDetect {
    public static void main(String[] args) throws Exception {
        // 0 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

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
                });

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
                });


        // 2.处理数据：实时对账 监控
        // 两条流 connect 起来，通过 txId 做一个匹配，匹配上就是对账成功
        // 对于同一笔订单的交易来说，业务系统 和 交易系统 的数据，哪个先来，是不一定的
        // TODO 一般两条流connect的时候，会做 keyby，为了要匹配的数据到一起
        // 可以先 keyby再 connect，也可以 先 connect，再 keyby
        ConnectedStreams<OrderEvent, TxEvent> orderTxCS = (orderDS.keyBy(order -> order.getTxId()))
                .connect(txDS.keyBy(tx -> tx.getTxId()));

        // 按照 txId进行分组，让相同txId的数据，到一起去
//        ConnectedStreams<OrderEvent, TxEvent> orderTxKS = orderTxCS.keyBy(
//                order -> order.getTxId(),
//                tx -> tx.getTxId());

        // 使用process进行处理
        SingleOutputStreamOperator<String> resultDS = orderTxCS.process(
                new CoProcessFunction<OrderEvent, TxEvent, String>() {
                    // 用来存放 交易系统 的数据
                    private Map<String, TxEvent> txMap = new HashMap<>();
                    // 用来存放 业务系统 的数据
                    private Map<String, OrderEvent> orderMap = new HashMap<>();

                    /**
                     * 处理业务系统的数据，来一条处理一条
                     * @param value
                     * @param ctx
                     * @param out
                     * @throws Exception
                     */
                    @Override
                    public void processElement1(OrderEvent value, Context ctx, Collector<String> out) throws Exception {
                        // 进入这个方法，说明来的数据是 业务系统的数据
                        // 判断 交易数据 来了没有？
                        // 通过 交易码 查询保存的 交易数据 => 如果不为空，说明 交易数据 已经来了，匹配上
                        TxEvent txEvent = txMap.get(value.getTxId());
                        if (txEvent == null) {
                            // 1.说明 交易数据 没来 => 等 ， 把自己临时保存起来
                            orderMap.put(value.getTxId(), value);
                        } else {
                            // 2.说明 交易数据 来了 => 对账成功
                            out.collect("订单" + value.getOrderId() + "对账成功");
                            // 对账成功，将保存的 交易数据 删掉
                            txMap.remove(value.getTxId());
                        }
                    }

                    /**
                     * 处理交易系统的数据，来一条处理一条
                     * @param value
                     * @param ctx
                     * @param out
                     * @throws Exception
                     */
                    @Override
                    public void processElement2(TxEvent value, Context ctx, Collector<String> out) throws Exception {
                        // 进入这个方法，说明来的数据是 交易系统 的数据
                        // 判断 业务数据 来了没有？
                        OrderEvent orderEvent = orderMap.get(value.getTxId());
                        if (orderEvent == null) {
                            // 1.说明 业务数据 没来 => 把自己 临时保存起来
                            txMap.put(value.getTxId(), value);
                        } else {
                            // 2.说明 业务数据 来了 => 对账成功
                            out.collect("订单" + orderEvent.getOrderId() + "对账成功");
                            // 对账成功，将保存的 业务数据 删掉
                            orderMap.remove(value.getTxId());
                        }
                    }
                }
        );

        resultDS.print();


        env.execute();
    }


}
