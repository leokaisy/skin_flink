package com.program.chapter6.window;

import com.program.chapter5.datasource.ClickSource;
import com.program.chapter5.datasource.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;

/**
 * @author : kaisy
 * @date : 2022/5/10 13:55
 * @Description : process window
 */
public class WindowProcessTest {
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

        // 3. 使用ProcessWindowFunction统计pv和uv
        ds.keyBy(data -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new UvCountByWindow())
                .print("process");


        // 4. 执行环境
        env.execute();
    }

    // 实现自定义的ProcessWindowFunction ， 输出一条统计信息
    public static class UvCountByWindow extends ProcessWindowFunction<Event,String,Boolean, TimeWindow> {
        @Override
        public void process(Boolean aBoolean, ProcessWindowFunction<Event, String, Boolean, TimeWindow>.Context context, Iterable<Event> elements, Collector<String> out) throws Exception {
            // 用一个HashSet保存user
            HashSet<String> userSet = new HashSet<>();

            // 从element中遍历数据，放至set中去重
            for (Event event : elements) {
                userSet.add(event.user);
            }

            // 结合窗口信息 输出内容
            Long start = context.window().getStart();
            Long end = context.window().getEnd();
            out.collect("窗口期为：" + new Timestamp(start) + " ~ " + new Timestamp(end) + "\n UV值为：" + userSet.size());

        }
    }
}
