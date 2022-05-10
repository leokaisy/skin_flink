package com.program.chapter6.window;

import com.program.chapter5.datasource.ClickSource;
import com.program.chapter5.datasource.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * @author : kaisy
 * @date : 2022/5/9 16:47
 * @Description : window
 */
public class WindowTest {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100);

        // 2. 从元素读取数据
        SingleOutputStreamOperator<Event> ds = env
//                .fromElements(new Event("Mary", "./cart", 1000L),
//                        new Event("Kally", "./tmt", 2000L),
//                        new Event("Felisa", "./pob.org", 3000L),
//                        new Event("Kally", "./vab", 3300L),
//                        new Event("Kally", "./home", 3500L),
//                        new Event("Felisa", "./home", 3600L),
//                        new Event("Kally", "./prod?id=1", 3800L),
//                        new Event("Kally", "./prod?id=2", 4200L)
//                )
                .addSource(new ClickSource())
                // 乱序流的watermark生成
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );

        // 窗口操作
        ds
                .map(new MapFunction<Event, Tuple2<String,Long>>() {
                    @Override
                    public Tuple2<String, Long> map(Event value) throws Exception {
                        return Tuple2.of(value.user, 1L);
                    }
                })
                .keyBy(data -> data.f0)
                // 窗口分配器
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))                     // 滚动事件时间窗口
//                .window(SlidingEventTimeWindows.of(Time.hours(1),Time.minutes(5)))      // 滑动实践时间窗口
//                .window(EventTimeSessionWindows.withGap(Time.seconds(2)))               // 事件时间会话窗口
//                .countWindow(10,2)                                                      // 滑动计数窗口
                // 规约窗口函数
                        .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                            @Override
                            public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                                return Tuple2.of(value1.f0,value1.f1 + value2.f1);
                            }
                        })
                .print("window")
                ;

        // 4. 执行环境
        env.execute();
    }
}
