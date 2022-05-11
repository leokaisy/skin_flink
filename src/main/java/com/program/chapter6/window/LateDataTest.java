package com.program.chapter6.window;

import com.program.chapter5.datasource.ClickSource;
import com.program.chapter5.datasource.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @author : kaisy
 * @date : 2022/5/11 9:57
 * @Description : delay time
 */
public class LateDataTest {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100);

        // 2. 从元素读取数据
        SingleOutputStreamOperator<Event> ds = env.socketTextStream("localhost",7777)
                .map(data -> {
                    String[] fields = data.split(",");
                    return new Event(fields[0].trim() , fields[1].trim() , Long.valueOf(fields[2].trim()));
                })
                // 乱序流的watermark生成
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );
        // 2.1 打印每条数据
        ds.print("data");

        // 定义一个输出标签
        OutputTag<Event> late = new OutputTag<>("late");

        // 3. 统计每个url的访问量
        SingleOutputStreamOperator<UrlViewCount> result = ds.keyBy(data -> data.url)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(late)
                .aggregate(new UrlCountViewTest.UrlViewCountAgg(), new UrlCountViewTest.UrlCountResult());

        // 3.2 打印结果数据
        result.print("result");
        // 3.3 打印侧输出流
        result.getSideOutput(late).print("late");

        // 4. 执行环境
        env.execute();
    }

}
