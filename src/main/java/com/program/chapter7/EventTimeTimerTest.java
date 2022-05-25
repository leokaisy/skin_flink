package com.program.chapter7;

import com.program.chapter5.datasource.ClickSource;
import com.program.chapter5.datasource.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * @author : kaisy
 * @date : 2022/5/24 10:54
 * @Description :
 */
public class EventTimeTimerTest {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 获取数据源
        SingleOutputStreamOperator<Event> stream = env.addSource(new CustomerSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));

        // 3. 数据处理
        stream.keyBy(data -> data.user)
                .process(new KeyedProcessFunction<String, Event, String>() {
                    @Override
                    public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {
                        long currTs = ctx.timestamp();
                        out.collect(ctx.getCurrentKey() + " 数据已到达，时间戳： " + new Timestamp(currTs) + ",watermark: " + ctx.timerService().currentWatermark());

                        // 注册一个10秒后的定时器
                        ctx.timerService().registerEventTimeTimer(currTs + 10 * 1000);
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect(ctx.getCurrentKey() + " 定时器触发，触发时间：" + new Timestamp(timestamp) + " watermark: " + ctx.timerService().currentWatermark());
                    }
                }).print();

        // 4. 执行环境
        env.execute();
    }

    // 自定义测试数据源
    public static class CustomerSource implements SourceFunction<Event>{

        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            // 直接发出测试数据
            ctx.collect(new Event("mary","./home",1000L));

            Thread.sleep(5000L);

            ctx.collect(new Event("Alice","./home",11000L));
        }

        @Override
        public void cancel() {

        }
    }
}
