package com.program.chapter9;

import com.program.chapter5.datasource.ClickSource;
import com.program.chapter5.datasource.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * @author : kaisy
 * @date : 2022/5/31 14:05
 * @Description : window
 */
public class FakeWindowExample {
    public static void main(String[] args) throws Exception{
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 添加数据源
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));

        stream.print("input: ");

        // 3. 进行数据处理
        // 3.1 统计每个用户的pv
        stream.keyBy(data -> data.url)
                .process(new FakeWindowResult(10000L))
                .print("url count: ");

        // 4. 执行环境
        env.execute();
    }

    // 实现自定义的KeyedProcessFunction
    public static class FakeWindowResult extends KeyedProcessFunction<String,Event,String> {
        // 窗口大小
        private Long windowSize;

        public FakeWindowResult(Long windowSize) {
            this.windowSize = windowSize;
        }

        // 定义一个mapState，用来保存每个窗口中统计的count值
        MapState<Long,Long> windowUrlCountState;

        @Override
        public void open(Configuration parameters) throws Exception {
            windowUrlCountState = getRuntimeContext().getMapState(new MapStateDescriptor<Long, Long>("window-count", Long.class, Long.class));
        }

        @Override
        public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {
            // 没来一条数据，根据时间戳判断属于那个窗口  创久分配器
            Long windowStart = value.timestamp / windowSize * windowSize;
            Long windowEnd = windowStart + windowSize;

            // 注册end - 1 的定时器
            ctx.timerService().registerEventTimeTimer(windowEnd - 1);

            // 更新状态
            if (windowUrlCountState.contains(windowStart)) {
                Long count = windowUrlCountState.get(windowStart);
                windowUrlCountState.put(windowStart, count + 1);
            } else {
                windowUrlCountState.put(windowStart , 1L);
            }
        }

        // 定时器触发时输出计算结果
        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            Long windowEnd = timestamp + 1;
            long windowStart = windowEnd - windowSize;
            Long count = windowUrlCountState.get(windowStart);

            out.collect("窗口：" + new Timestamp(windowStart) + " ~ " + new Timestamp(windowEnd) + "\n"
                    + "url: " + ctx.getCurrentKey() + "\n"
                    + "count: " + count);

            // 模拟窗口的关闭，清除map中对应的key-value
            windowUrlCountState.remove(windowStart);
        }
    }

}
