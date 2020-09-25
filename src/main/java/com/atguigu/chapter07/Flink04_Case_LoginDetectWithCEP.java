package com.atguigu.chapter07;

import com.atguigu.bean.LoginEvent;
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
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/9/24 14:20
 */
public class Flink04_Case_LoginDetectWithCEP {
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

        // 2.按照 统计维度 分组： 用户
        KeyedStream<LoginEvent, Long> loginKS = loginDS.keyBy(data -> data.getUserId());

        //TODO CEP处理
        // 1. 定义规则
        Pattern<LoginEvent, LoginEvent> pattern = Pattern
                .<LoginEvent>begin("firstFail")
                .where(new IterativeCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent, Context<LoginEvent> context) throws Exception {
                        return "fail".equals(loginEvent.getEventType());
                    }
                })
                .next("secondFail")
                .where(new IterativeCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent, Context<LoginEvent> context) throws Exception {
                        return "fail".equals(loginEvent.getEventType());
                    }
                })
                .within(Time.seconds(2));

        // 2. 应用规则
        PatternStream<LoginEvent> loginPS = CEP.pattern(loginKS, pattern);

        // 3. 获取匹配上的结果
        SingleOutputStreamOperator<String> resultDS = loginPS.select(new PatternSelectFunction<LoginEvent, String>() {
            @Override
            public String select(Map<String, List<LoginEvent>> map) throws Exception {
                LoginEvent firstFail = map.get("firstFail").iterator().next();
                LoginEvent secondFail = map.get("secondFail").iterator().next();
                return "用户" + firstFail.getUserId() + "在2s内连续登陆失败2次，可能为恶意登陆！！！";
            }
        });

        resultDS.print();

        env.execute();
    }
}
