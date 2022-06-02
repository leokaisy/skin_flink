package com.program.chapter9;

import com.program.chapter5.datasource.ClickSource;
import com.program.chapter5.datasource.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author : kaisy
 * @date : 2022/5/30 17:04
 * @Description : state
 */
public class StateTest {
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

        // 3. 数据处理
        stream.keyBy(data -> data.user)
                .flatMap(new MyFlatMap())
                .print();

        // 4. 执行环境
        env.execute();
    }

    // 实现自定义的FlatMapFunction ，用于keyed state 测试
    public static class MyFlatMap extends RichFlatMapFunction<Event,String> {
        // 定义状态
        ValueState<Event> myValueState;
        ListState<Event> myListState;
        MapState<String,Long> myMapState;
        ReducingState<Event> myReduceState;
        AggregatingState<Event,String> myAggregateState;

        // 增加一个本地变量进行对比
        Long count = 0L;

        @Override
        public void open(Configuration parameters) throws Exception {

            ValueStateDescriptor<Event> valueStateDescriptor = new ValueStateDescriptor<>("my-state", Event.class);

            myValueState = getRuntimeContext().getState(valueStateDescriptor);
            myListState = getRuntimeContext().getListState(new ListStateDescriptor<Event>("my-list", Event.class));
            myMapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Long>("my-map", String.class , Long.class));

            myReduceState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<Event>("my-reduce", new ReduceFunction<Event>() {
                @Override
                public Event reduce(Event value1, Event value2) throws Exception {
                    return new Event(value1.user , value1.url , value2.timestamp);
                }
            }, Event.class));
            myAggregateState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Event, Long, String>("my-aggregate", new AggregateFunction<Event, Long, String>() {
                @Override
                public Long createAccumulator() {
                    return 0L;
                }

                @Override
                public Long add(Event value, Long accumulator) {
                    return accumulator +1;
                }

                @Override
                public String getResult(Long accumulator) {
                    return "count : " + accumulator;
                }

                @Override
                public Long merge(Long a, Long b) {
                    return a + b;
                }
            }, Long.class));


            // 配置状态的TTL
            StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(1))
                    .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                    .build();

            valueStateDescriptor.enableTimeToLive(ttlConfig);
        }

        @Override
        public void flatMap(Event value, Collector<String> out) throws Exception {
            // 访问和更新状态
//            System.out.println(myValueState.value());
            myValueState.update(value);
//            System.out.println("my value" + myValueState.value());

            // List
            myListState.add(value);

            // map
            myMapState.put(value.user, myMapState.get(value.user) == null ? 1 : myMapState.get(value.user) + 1);
            System.out.println("my map state : " + value.user + " " + myMapState.get(value.user));

            // aggregate
            myAggregateState.add(value);
            System.out.println("my agg state : " + myAggregateState.get());

            // reduce
            myReduceState.add(value);
            System.out.println("my reducing state : " + myReduceState.get());

            // 对本地变量进行更新
            count++;
            System.out.println("count : " + count);
        }
    }
}