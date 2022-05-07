package com.program.chapter5.sink;

import com.program.chapter5.datasource.Event;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author : kaisy
 * @date : 2022/5/7 14:21
 * @Description : es sink
 */
public class SinkToES {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 从元素读取数据
        DataStreamSource<Event> ds = env.fromElements(new Event("Mary","./cart",1000L),
                new Event("Kally", "./tmt", 2000L),
                new Event("Felisa", "./pob.org", 3000L),
                new Event("Kally", "./vab", 3300L),
                new Event("Kally", "./home", 3500L),
                new Event("Felisa", "./home", 3600L),
                new Event("Kally", "./prod?id=1", 3800L),
                new Event("Kally", "./prod?id=2", 4200L)
        );

        // 3. 将数据写入es
        // 3.1 定义hosts的列表
        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("hadoop101", 9200));
        // 3.2 定义ElasticSearchSinkFunction
        ElasticsearchSinkFunction<Event> elasticsearchSinkFunction = new ElasticsearchSinkFunction<Event>() {
            @Override
            public void process(Event event, RuntimeContext ctx, RequestIndexer indexer) {
                HashMap<String, String> map = new HashMap<>();
                map.put(event.user, event.url);

                // 构建一个IndexRequest
                IndexRequest request = Requests.indexRequest()
                        .index("clicks")
                        .type("type")
                        .source(map);

                indexer.add(request);
            }
        };
        // 3.3 写入es
        ds.addSink(new ElasticsearchSink.Builder<>(httpHosts, elasticsearchSinkFunction).build());

        // 4. 执行环境
        env.execute();

    }
}
