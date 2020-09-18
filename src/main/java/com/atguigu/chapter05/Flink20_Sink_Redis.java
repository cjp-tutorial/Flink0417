package com.atguigu.chapter05;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/9/16 15:29
 */
public class Flink20_Sink_Redis {
    public static void main(String[] args) throws Exception {

        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.从文件读取数据
        DataStreamSource<String> inputDS = env
                .readTextFile("input/sensor-data.log");
//                .socketTextStream("localhost", 9999);


        //TODO 数据 Sink到 Redis

        FlinkJedisPoolConfig jedisConfig = new FlinkJedisPoolConfig.Builder()
                .setHost("hadoop102")
                .setPort(6379)
                .build();

        inputDS.addSink(
                new RedisSink<String>(
                        jedisConfig,
                        new RedisMapper<String>() {
                            // redis 的命令: key是最外层的 key
                            @Override
                            public RedisCommandDescription getCommandDescription() {
                                return new RedisCommandDescription(RedisCommand.HSET,"sensor0421");
                            }

                            // Hash类型：这个指定的是 hash 的key
                            @Override
                            public String getKeyFromData(String data) {
                                String[] datas = data.split(",");
                                return datas[1];
                            }

                            // Hash类型：这个指定的是 hash 的 value
                            @Override
                            public String getValueFromData(String data) {
                                String[] datas = data.split(",");
                                return datas[2];
                            }
                        }
                )
        );


        env.execute();
    }


    public static class MyKeySelector implements KeySelector<WaterSensor, String> {

        @Override
        public String getKey(WaterSensor value) throws Exception {
            return value.getId();
        }
    }


}
