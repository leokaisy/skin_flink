package com.program.chapter7;

import com.program.chapter5.datasource.ClickSource;
import com.program.chapter5.datasource.Event;
import com.program.chapter6.window.UrlCountViewTest;
import com.program.chapter6.window.UrlViewCount;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * @author : kaisy
 * @date : 2022/5/24 15:17
 * @Description : top n
 */
public class TopNExample {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 获取数据源
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));

        // 3. 按照url分组，统计窗口内每个url的访问量
        SingleOutputStreamOperator<UrlViewCount> urlCountStream = stream.keyBy(data -> data.url)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5))) // 定义滑动窗口
                .aggregate(new UrlCountViewTest.UrlViewCountAgg(), new UrlCountViewTest.UrlCountResult());

        urlCountStream.print();

        // 3.2 对于同一窗口统计出的访问量，进行收集和排序


        // 4. 执行环境
        env.execute();
    }
}
