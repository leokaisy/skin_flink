package com.program.chapter5.transform;

import com.program.chapter5.datasource.Event;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author : kaisy
 * @date : 2022/5/6 16:07
 * @Description : aggregation
 */
public class SimpleAggTest {
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

        // 3. 按键分组后进行聚合，提取当前用户最近一次访问数据
        ds.keyBy(new KeySelector<Event, String>() {
            @Override
            public String getKey(Event event) {
                return event.user;
            }
        }).max("timestamp")
                .print("max: ");

        // 3.2 lambda表达式
        ds.keyBy(x -> x.user).maxBy("timestamp").print("maxBy: ");

        // 4. 开始执行
        env.execute();
    }
}
