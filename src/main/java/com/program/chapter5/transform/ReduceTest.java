package com.program.chapter5.transform;

import com.program.chapter5.datasource.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author : kaisy
 * @date : 2022/5/6 17:36
 * @Description : reduce
 */
public class ReduceTest{
    public static void main(String[]args) throws Exception{
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 从元素读取数据
        DataStreamSource<Event> ds = env.fromElements(new Event("Mary","./cart",1000L),
                new Event("Kally", "./tmt", 2000L),
                new Event("Felisa", "./pob.org", 3000L),
                new Event("Kally", "./vab", 3300L),
                new Event("Felisa", "./home", 3600L),
                new Event("Felisa", "./prod?id=100", 3600L),
                new Event("Kally", "./home", 3500L),
                new Event("Kally", "./prod?id=1", 3800L),
                new Event("Kally", "./prod?id=2", 4200L)
        );

        // 3. 访问量计算聚合 统计每个用户的访问频次
        // 3.1
        SingleOutputStreamOperator<Tuple2<String, Long>> clickByUser = ds.map(new MapFunction<Event, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(Event event) throws Exception {
                        return Tuple2.of(event.user, 1L);
                    }
                }).keyBy(data -> data.f0)
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                    }
                });

        // 3.2 选取当前最活跃的用户
        SingleOutputStreamOperator<Tuple2<String, Long>> result = clickByUser.keyBy(data -> "key")
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        return value1.f1 > value2.f1 ? value1 : value2;
                    }
                });

        // 3.3 打印结果
        result.print("active: ");

        // 4. 开始执行
        env.execute();
    }
}
