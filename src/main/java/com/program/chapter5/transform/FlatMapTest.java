package com.program.chapter5.transform;

import com.program.chapter5.datasource.Event;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author : kaisy
 * @date : 2022/5/6 15:24
 * @Description : flatmap
 */
public class FlatMapTest {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 从元素读取数据
        DataStreamSource<Event> ds = env.fromElements(
                new Event("Kally", "./tmt", 3000L),
                new Event("Felisa", "./pob.org", 4000L)
        );

        // 3. 扁平化映射
        // 3.1 实现一个自定义的FlatMapFunction
        ds.flatMap(new MyFlatMap()).print("1");

        // 3.2 传入一个Lambda表达式
        ds.flatMap((Event value, Collector<String> out) -> {
            if (value.user.equals("Kally")) {
                out.collect(value.user);
            } else if (value.user.equals("Felisa")) {
                out.collect(value.user);
                out.collect(value.url);
            }
        }).returns(new TypeHint<String>() {})
                .print("2");

        // 4. 执行
        env.execute();
    }
    public static class MyFlatMap implements FlatMapFunction<Event, String> {
        @Override
        public void flatMap(Event value, Collector<String> out) throws Exception {
            out.collect(value.user);
            out.collect(value.url);
            out.collect(value.timestamp.toString());
        }
    }
}
