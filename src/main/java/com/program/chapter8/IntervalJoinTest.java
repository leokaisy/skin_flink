package com.program.chapter8;

import com.program.chapter5.datasource.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author : kaisy
 * @date : 2022/5/30 11:07
 * @Description : interval join
 */
public class IntervalJoinTest {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2.1 获取第二个数据源
        SingleOutputStreamOperator<Tuple2<String, Integer>> orderStream = env.fromElements(
                Tuple2.of("Marry", 1000),
                Tuple2.of("Kally", 1000),
                Tuple2.of("Felisa", 2000),
                Tuple2.of("Kally", 2000),
                Tuple2.of("Pom", 5100)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Integer>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Integer>>() {
                    @Override
                    public long extractTimestamp(Tuple2<String, Integer> element, long recordTimestamp) {
                        return element.f1;
                    }
                }));

        // 2.2 获取第一个数据源
        SingleOutputStreamOperator<Event> clickStream = env.fromElements(
                new Event("Mary", "./cart", 2000L),
                new Event("Kally", "./tmt", 3000L),
                new Event("Felisa", "./pob.org", 3500L),
                new Event("Kally", "./vab", 2500L),
                new Event("Felisa", "./home", 3600L),
                new Event("Felisa", "./prod?id=100", 3600L),
                new Event("Kally", "./home", 35000L),
                new Event("Kally", "./prod?id=1", 23000L),
                new Event("Kally", "./prod?id=2", 33000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                }));



        // 3.3 合并连接两个流
        orderStream.keyBy(data -> data.f0)
                .intervalJoin(clickStream.keyBy(data -> data.user))
                .between(Time.seconds(5), Time.seconds(10))
                .process(new ProcessJoinFunction<Tuple2<String, Integer>, Event, String>() {
                    @Override
                    public void processElement(Tuple2<String, Integer> left, Event right, ProcessJoinFunction<Tuple2<String, Integer>, Event, String>.Context ctx, Collector<String> out) throws Exception {
                        out.collect(right + " -> " + left);
                    }
                })
                .print();



        // 4. 执行环境
        env.execute();
    }
}
