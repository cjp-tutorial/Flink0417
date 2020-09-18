package com.atguigu.chapter05;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/9/16 15:29
 */
public class Flink21_Sink_ES {
    public static void main(String[] args) throws Exception {

        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.从文件读取数据
        DataStreamSource<String> inputDS = env
                .readTextFile("input/sensor-data.log");
//                .socketTextStream("localhost", 9999);


        //TODO 数据 Sink到 ES

        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("hadoop102",9200));
        httpHosts.add(new HttpHost("hadoop103",9200));
        httpHosts.add(new HttpHost("hadoop104",9200));

        ElasticsearchSink<String> esSink = new ElasticsearchSink.Builder<String>(
                httpHosts,
                new ElasticsearchSinkFunction<String>() {
                    @Override
                    public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
                        // 将数据放在Map中
                        Map<String, String> dataMap = new HashMap<>();
                        dataMap.put("data", element);
                        // 创建 IndexRequest =》 指定index，指定type，指定source
                        IndexRequest indexRequest = Requests.indexRequest("sensor0421").type("reading").source(dataMap);
                        // 添加到 RequestIndexer
                        indexer.add(indexRequest);
                    }
                }
        )
                .build();

        inputDS.addSink(esSink);


        env.execute();
    }


    public static class MyKeySelector implements KeySelector<WaterSensor, String> {

        @Override
        public String getKey(WaterSensor value) throws Exception {
            return value.getId();
        }
    }


}
