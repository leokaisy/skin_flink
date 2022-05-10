package com.program.chapter6.watermark;

import com.program.chapter5.datasource.Event;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author : kaisy
 * @date : 2022/5/9 16:33
 * @Description : customized watermark
 */
// 自定义水位线的产生
public class CustomerWatermarkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.fromElements(new Event("Mary", "./cart", 1000L),
                        new Event("Kally", "./tmt", 2000L),
                        new Event("Felisa", "./pob.org", 3000L),
                        new Event("Kally", "./vab", 3300L),
                        new Event("Kally", "./home", 3500L),
                        new Event("Felisa", "./home", 3600L),
                        new Event("Kally", "./prod?id=1", 3800L),
                        new Event("Kally", "./prod?id=2", 4200L)
                )
                .assignTimestampsAndWatermarks(new CustomWatermarkStrategy())
                .print();

        env.execute();
    }

    public static class CustomWatermarkStrategy implements WatermarkStrategy<Event> {
        @Override
        public TimestampAssigner<Event>
        createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return new SerializableTimestampAssigner<Event>() {
                @Override
                public long extractTimestamp(Event element, long recordTimestamp)
                {
                    return element.timestamp; // 告诉程序数据源里的时间戳是哪一个字段
                }
            };
        }

        @Override
        public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new CustomPeriodicGenerator();
        }
    }

    public static class CustomPeriodicGenerator implements WatermarkGenerator<Event> {
        private Long delayTime = 5000L; // 延迟时间
        private Long maxTs = Long.MIN_VALUE + delayTime + 1L; // 观察到的最大时间戳

        @Override
        public void onEvent(Event event, long eventTimestamp, WatermarkOutput
                output) {
            // 每来一条数据就调用一次
            maxTs = Math.max(event.timestamp, maxTs); // 更新最大时间戳
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // 发射水位线，默认 200ms 调用一次
            output.emitWatermark(new Watermark(maxTs - delayTime - 1L));
        }
    }
}

