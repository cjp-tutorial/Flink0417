package com.atguigu.chapter08;

import com.atguigu.bean.HotItemCountWithWindowEnd;
import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/9/23 10:13
 */
public class Flink09_SQL_HotItemsAnalysis {
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

        // TODO 使用 TableAPI和SQL实现TopN =》 TopN只能用 Blink
        // 1.创建表的执行环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // 2.从流中获取 Table对象
        Table table = tableEnv.fromDataStream(userBehaviorDS, "itemId,behavior,timestamp.rowtime");

        // 3.使用 TableAPI准备数据:过滤、开窗、分组、聚合、打windowEnd标签
        Table aggTable = table
                .filter("behavior == 'pv'")
                .window(Slide.over("1.hours").every("5.minutes").on("timestamp").as("w"))
                .groupBy("itemId,w")
                .select("itemId,count(itemId) as cnt,w.end as windowEnd");
        // 先转成流 =》防止自动转换类型，出现匹配不上的情况，先转成流，指定了Row类型，后面类型就能统一
        DataStream<Row> aggDS = tableEnv.toAppendStream(aggTable, Row.class);

        // 4.使用 SQL实现 TopN的排序
        tableEnv.createTemporaryView("aggTable", aggDS, "itemId,cnt,windowEnd");
        Table top3Table = tableEnv.sqlQuery("select " +
                "* " +
                "from (" +
                "    select " +
                "    *," +
                "    row_number() over(partition by windowEnd order by cnt desc) as ranknum " +
                "    from aggTable" +
                ") " +
                "where ranknum <= 3");

        tableEnv.toRetractStream(top3Table, Row.class).print();

        env.execute();
    }
}
