package com.atguigu.chapter08;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/9/27 9:34
 */
public class Flink08_SQL_Operator {
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
        SingleOutputStreamOperator<WaterSensor> sensorDS1 = env
                .readTextFile("input/sensor-data-cep.log")
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
        tableEnv.createTemporaryView("sensorTable", sensorDS, "id,ts,vc");
        tableEnv.createTemporaryView("sensorTable1", sensorDS1, "id,ts,vc");

        //TODO 3. 使用 SQL进行处理
        Table resultTable = tableEnv
//                .sqlQuery("select * from sensorTable"); // 查询
//                .sqlQuery("select * from sensorTable where id='sensor_1'"); // 条件
//                .sqlQuery("select id,count(id) from sensorTable group by id"); // 分组
//                .sqlQuery("select " +
//                        "* " +
//                        "from sensorTable s1 " +
//                        "left join sensorTable1 s2 " +
//                        "on s1.id=s2.id"); //连接
                .sqlQuery("select " +
                        "* " +
                        "from " +
                        "(select * from sensorTable)" +
                        "union " +
                        "(select * from sensorTable1)"); // union合并 =》 去重

        tableEnv
                .toRetractStream(resultTable, Row.class)
                .print();

        env.execute();
    }
}
