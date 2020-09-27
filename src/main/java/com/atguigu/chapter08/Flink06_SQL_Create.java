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

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/9/27 9:34
 */
public class Flink06_SQL_Create {
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

        // TODO Table API
        // 1. 创建 表执行环境
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useOldPlanner() // 使用官方的 planner
//                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // TODO 获取 Table对象
        // TODO 方式1. 将 DataStream 转换成 Table对象
        Table table = tableEnv.fromDataStream(sensorDS, "id,ts,vc");
        // TODO 方式2  从表名获取 Table对象
        tableEnv.createTemporaryView("sensorTable", sensorDS, "id,ts,vc");
        Table sensorTable = tableEnv.from("sensorTable");

        // 3. 保存到 本地文件系统
        tableEnv
                .connect(new FileSystem().path("out/flink.txt"))
                .withFormat(new OldCsv().fieldDelimiter("、"))
                .withSchema(
                        new Schema()
                                .field("id", DataTypes.STRING())
                                .field("timestamp", DataTypes.BIGINT())
                                .field("hahaha", DataTypes.INT())
                )
                .createTemporaryTable("fsTable");

        //TODO 方式2 从表名获取 Table对象
        Table fsTable = tableEnv.from("fsTable");

        // 使用 SQL保存数据到 文件（外部系统）
        tableEnv.sqlUpdate("INSERT INTO fsTable SELECT * FROM sensorTable" );

        env.execute();
    }
}
