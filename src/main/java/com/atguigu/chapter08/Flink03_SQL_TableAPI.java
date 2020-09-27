package com.atguigu.chapter08;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
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
public class Flink03_SQL_TableAPI {
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

        // 2. 把 DataStream 转换成 Table
        Table sensorTable = tableEnv.fromDataStream(sensorDS, "id,ts as timestamp,vc");

        // 3. 使用 Table API进行处理
        Table resultTable = sensorTable
                .select("id,vc");

        // 4. 保存到 本地文件系统
        // 连接外部系统，将外部系统抽象成 一个 Table对象，需要指定存储格式、表的结构信息（字段名、类型）、表名
        // 第一步 connect() 外部系统的连接描述器，官方有 FS、Kafka、ES
        // 第二步 withFormat  指定 外部系统 数据的存储格式
        // 第三步 withSchema 要抽象成的 Table 的 Schema信息，有 字段名、字段类型
        // 第四步 createTemporaryTable 给抽象成的 Table 一个 表名
        tableEnv
                .connect(new FileSystem().path("out/flink.txt"))
                .withFormat(new OldCsv().fieldDelimiter("|"))
                .withSchema(
                        new Schema()
                                .field("id", DataTypes.STRING())
                                .field("hahaha", DataTypes.INT())
                )
                .createTemporaryTable("fsTable");

        // 使用 TableAPI里的 insertInto, 把一张表的数据 插入到 另一张表(外部系统抽象成的 Table)
        resultTable.insertInto("fsTable");

        env.execute();
    }
}
