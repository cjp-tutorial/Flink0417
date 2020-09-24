package com.atguigu.chapter06;

import com.atguigu.bean.LoginEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/9/24 14:20
 */
public class Flink30_Case_LoginDetect {
    public static void main(String[] args) throws Exception {
        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 1.读取数据
        SingleOutputStreamOperator<LoginEvent> loginDS = env
                .readTextFile("input/LoginLog.csv")
                .map(new MapFunction<String, LoginEvent>() {
                    @Override
                    public LoginEvent map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new LoginEvent(
                                Long.valueOf(datas[0]),
                                datas[1],
                                datas[2],
                                Long.valueOf(datas[3])
                        );
                    }
                })
                .assignTimestampsAndWatermarks(
                        new BoundedOutOfOrdernessTimestampExtractor<LoginEvent>(Time.seconds(10)) {
                            @Override
                            public long extractTimestamp(LoginEvent element) {
                                return element.getEventTime() * 1000L;
                            }
                        }
                );


        //TODO
        // 1.不应该过滤 => 破坏了连续性
        // 2. 乱序场景下有问题 => 按照时间顺序是 F、F、S => 但是收到数据是 F、S、F
        // 3. 如果是 2s内 连续5次失败呢？

        // 2.处理数据
        // 2.1 过滤
        SingleOutputStreamOperator<LoginEvent> filterDS = loginDS.filter(data -> "fail".equals(data.getEventType()));
        // 2.2 按照 统计维度 分组：用户
        KeyedStream<LoginEvent, Long> loginKS = filterDS.keyBy(data -> data.getUserId());
        // 2.3 分析是否恶意登陆
        SingleOutputStreamOperator<String> resultDS = loginKS.process(new LoginFailDetect());

        resultDS.print();

        env.execute();
    }

    public static class LoginFailDetect extends KeyedProcessFunction<Long, LoginEvent, String> {

        private ValueState<LoginEvent> failState;

        @Override
        public void open(Configuration parameters) throws Exception {
            failState = getRuntimeContext().getState(new ValueStateDescriptor<LoginEvent>("failState", LoginEvent.class));
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
        public void processElement(LoginEvent value, Context ctx, Collector<String> out) throws Exception {
            // 判断当前是第几次失败 =》 是第一次，还是第二次？
            if (failState.value() == null) {
                // 1.说明当前数据是第一次失败 => 保存起来
                failState.update(value);
            } else {
                // 2.说明当前数据不是第一次失败 =》 判断时间是否超过2s
                if (Math.abs(value.getEventTime() - failState.value().getEventTime()) <= 2) {
                    // 告警
                    out.collect("用户" + value.getUserId() + "在2s内登陆失败2次，可能为恶意登陆！");
                }
                // 不管是否达到告警条件，都要把当前的失败保存起来，后面还可以继续判断
                failState.update(value);
            }

        }
    }
}
