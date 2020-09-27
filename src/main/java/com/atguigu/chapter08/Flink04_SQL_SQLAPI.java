package com.atguigu.chapter08;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/9/27 9:34
 */
public class Flink04_SQL_SQLAPI {
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

        // 2. 把 DataStream 转换成 Table
        Table sensorTable = tableEnv.fromDataStream(sensorDS, "id,ts as timestamp,vc");
        // 3. 给 Table 命名
        // 通过 createTemporaryView
        // => 可以将 DataStream 转成 Table对象，并且给一个 表名,可以指定 字段名
        // => 也可以将 Table对象，给一个 表名，不能再指定 字段名
//        tableEnv.createTemporaryView("sensorTable", sensorTable);
        tableEnv.createTemporaryView("sensorTable", sensorDS,"id,ts,vc");

        //TODO 3. 使用 SQL进行处理
//        Table resultTable = tableEnv.sqlQuery("select * from " + sensorTable);
        Table resultTable = tableEnv.sqlQuery("select * from sensorTable");

        tableEnv
                .toAppendStream(resultTable, Row.class)
                .print();

        env.execute();
    }
}
