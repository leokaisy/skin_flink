package com.program.chapter9;

import com.program.chapter5.datasource.ClickSource;
import com.program.chapter5.datasource.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author : kaisy
 * @date : 2022/6/1 10:37
 * @Description : average
 */
public class AverageTimestampExample {
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

        // 3. 自定义实现平均时间戳的统计
        // 3.1 统计每个用户的点击频次
        stream.keyBy(data -> data.user)
                .flatMap(new AvgTsResult(5L))
                .print("url count: ");

        // 4. 执行环境
        env.execute();
    }

    // 实现自定义的RichFlatMapFunction
    public static class AvgTsResult extends RichFlatMapFunction<Event,String>{
        private Long count;

        public AvgTsResult(Long count) {
            this.count = count;
        }

        // 定义一个聚合的状态，用来保存平均时间戳
        AggregatingState<Event, Long> avgTsState;
        // 定义一个值状态，保存用户的访问次数
        ValueState<Long> countState ;

        @Override
        public void open(Configuration parameters) throws Exception {
            avgTsState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Event, Tuple2<Long, Long>, Long>(
                    "avg-ts",
                    new AggregateFunction<Event, Tuple2<Long, Long>, Long>() {
                        @Override
                        public Tuple2<Long, Long> createAccumulator() {
                            return Tuple2.of(0L, 0L);
                        }

                        @Override
                        public Tuple2<Long, Long> add(Event value, Tuple2<Long, Long> accumulator) {
                            return Tuple2.of(accumulator.f0 + value.timestamp, accumulator.f1 + 1);
                        }

                        @Override
                        public Long getResult(Tuple2<Long, Long> accumulator) {
                            return accumulator.f0 / accumulator.f1;
                        }

                        @Override
                        public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
                            return null;
                        }
                    },
                    Types.TUPLE(Types.LONG, Types.LONG)));

            countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count", Long.class));
        }

        @Override
        public void flatMap(Event value, Collector<String> out) throws Exception {
            // 没来一条数据，curr count 加1
            Long currCount = countState.value();
            if(currCount == null ){
                currCount = 1L;
            }else {
                currCount++;
            }

            // 更新状态
            countState.update(currCount);
            avgTsState.add(value);

            // 如果到达count次数就输出结果
            if (currCount.equals(count)) {
                out.collect(value.user + "过去" + count + "次访问平均时间戳为: " + avgTsState.get());
                // 清理状态
                countState.clear();
                avgTsState.clear();
            }
        }
    }
}
