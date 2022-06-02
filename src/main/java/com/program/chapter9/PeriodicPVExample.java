package com.program.chapter9;

import com.program.chapter5.datasource.ClickSource;
import com.program.chapter5.datasource.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author : kaisy
 * @date : 2022/5/30 17:51
 * @Description : state calculate
 */
public class PeriodicPVExample {
    public static void main(String[] args) throws Exception {
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
        stream.keyBy(data -> data.user)
                .process(new PeriodicPVResult())
                .print("state pv: ");

        // 4. 执行环境
        env.execute();
    }

    // 实现自定义的KeyedProcessFunction
    public static class PeriodicPVResult extends KeyedProcessFunction<String , Event , String>{
        // 定义状态，保存当前pv的统计值，以及有没有定时器
        ValueState<Long> countState;
        ValueState<Long> timeState;


        @Override
        public void open(Configuration parameters) throws Exception {
            countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count", Long.class));
            timeState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("time-ts", Long.class));
        }

        @Override
        public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {
            // 没来一条数据，就更新对应的count值
            Long count = countState.value();
            countState.update( count == null ? 1 : count + 1);

            // 如果没有注册过的话，注册定时器
            if (timeState.value() == null){
                  ctx.timerService().registerEventTimeTimer(value.timestamp + 10 * 1000L);
                  timeState.update(value.timestamp + 10 * 1000L);
            }
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发，输出一次统计结果
            out.collect(ctx.getCurrentKey() + " pv : " + countState.value());
            // 清空状态
            timeState.clear();
            // 重新注册定时器
            ctx.timerService().registerEventTimeTimer(timestamp + 10 * 1000L);
            timeState.update(timestamp + 10 * 1000L);
        }
    }
}
