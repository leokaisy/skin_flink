package com.program.chapter6.window;

import com.program.chapter5.datasource.ClickSource;
import com.program.chapter5.datasource.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.HashSet;

/**
 * @author : kaisy
 * @date : 2022/5/10 9:41
 * @Description : agg func sample
 */
public class WindowAggregateSampleTest {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100);

        // 2. 从元素读取数据
        SingleOutputStreamOperator<Event> ds = env.addSource(new ClickSource())
                // 乱序流的watermark生成
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );

        ds.print("data");

        // 3. 所有数据放在一起统计pv和uv
        ds.keyBy(data -> true)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2)))
                .aggregate(new AvgPvAgg())
                .print("agg");


        // 4. 执行环境
        env.execute();
    }

    // 自定义一个AggregateFunction 用Long保存pv hashset保存uv去重
    public static class AvgPvAgg implements AggregateFunction<Event, Tuple2<Long , HashSet<String>>,Double> {

        @Override
        public Tuple2<Long, HashSet<String>> createAccumulator() {
            return Tuple2.of(0L , new HashSet<>());
        }

        @Override
        public Tuple2<Long, HashSet<String>> add(Event value, Tuple2<Long, HashSet<String>> accumulator) {
            // 每来一条数据，pv个数加一，将user放入HashSet中
            accumulator.f1.add(value.user);
            return Tuple2.of(accumulator.f0 + 1L , accumulator.f1);
        }

        @Override
        public Double getResult(Tuple2<Long, HashSet<String>> accumulator) {
            // 窗口触发时，输出pv和uv的比值
            return (double) (accumulator.f0 / accumulator.f1.size());
        }

        @Override
        public Tuple2<Long, HashSet<String>> merge(Tuple2<Long, HashSet<String>> a, Tuple2<Long, HashSet<String>> b) {
            a.f1.addAll(b.f1);
            return Tuple2.of(a.f0 + b.f0 , a.f1);
        }
    }
}
