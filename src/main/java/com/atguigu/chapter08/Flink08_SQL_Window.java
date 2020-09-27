package com.atguigu.chapter08;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/9/27 9:34
 */
public class Flink08_SQL_Window {
    public static void main(String[] args) throws Exception {
        // 0 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 1. 读取数据、转换
        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .readTextFile("input/sensor-data.log")
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));

                    }
                })
                .assignTimestampsAndWatermarks(
                        new AscendingTimestampExtractor<WaterSensor>() {
                            @Override
                            public long extractAscendingTimestamp(WaterSensor element) {
                                return element.getTs() * 1000L;
                            }
                        }
                );


        // TODO SQL操作 Table
        // 1. 创建 表执行环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useOldPlanner() // 使用官方的 planner
//                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // 2. 把 DataStream 转换成 Table,命名
        tableEnv.createTemporaryView("sensorTable", sensorDS, "id,ts.rowtime as rt,vc");
//        tableEnv.createTemporaryView("sensorTable", sensorDS, "id,ts.proctime,vc");
        Table sensorTable = tableEnv.from("sensorTable");

        //TODO 3. 使用 TableAPI 开窗（Group Window）
        // 1.要在字段里 指定 用来分组（按时间间隔）或者排序（按行数）的时间字段 => 字段名.rowtime 或 字段名.proctime,还可以起个别名
        // 2.table调用window方法
//                => 首先，指定窗口类型：Tumble、Slide、Session
//                => 然后，指定窗口的参数：over("xxx.minutes")窗口长度、every("xxx.minutes")滑动步长
//                => 接着，指定 用来分组（按时间间隔）或者排序（按行数）的时间字段 => 指定为 rowtime或 proctime的字段
//                => 最后，给窗口起一个别名
        // 3.必须把 窗口 放在 分组字段里
        // 4.可以用 窗口.start 窗口.end 获取窗口的开始和结束时间
//        Table resultTable = sensorTable
////                .window(Tumble.over("5.seconds").on("rt").as("w"))
//                .window(Slide.over("5.seconds").every("2.seconds").on("rt").as("w"))
//                .groupBy("id,w")
//                .select("id,count(id),w.start,w.end");

//        Table resultTable = tableEnv
//                .sqlQuery("select " +
//                        "id," +
//                        "HOP_END(atime,INTERVAL '1' HOUR,INTERVAL '5' MINUTE) " +
//                        "from sensorTable " +
//                        "group by HOP(atime,INTERVAL '1' HOUR,INTERVAL '5' MINUTE),id");

        // TODO OverWindow
        // TableAPI
//        Table resultTable = sensorTable
//                .window(
//                        Over
//                                .partitionBy("id")
//                                .orderBy("rt")
//                                .preceding("UNBOUNDED_RANGE")
//                                .following("CURRENT_RANGE")
//                                .as("ow"))
//                .select("id,count(id) over ow");

        // SQL
        Table resultTable = tableEnv.sqlQuery("select id," +
                "count(id) over(partition by id order by rt ROWS BETWEEN 2 PRECEDING AND CURRENT ROW ) " +
                "from sensorTable");

        tableEnv.toRetractStream(resultTable, Row.class).print();

        env.execute();
    }
}
