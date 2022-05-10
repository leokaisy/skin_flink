package com.program.chapter6.watermark;

import com.program.chapter5.datasource.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * @author : kaisy
 * @date : 2022/5/9 11:15
 * @Description : watermark
 */
public class WatermarkTest {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100);

        // 2. 从元素读取数据
        SingleOutputStreamOperator<Event> ds = env.fromElements(new Event("Mary", "./cart", 1000L),
                        new Event("Kally", "./tmt", 2000L),
                        new Event("Felisa", "./pob.org", 3000L),
                        new Event("Kally", "./vab", 3300L),
                        new Event("Kally", "./home", 3500L),
                        new Event("Felisa", "./home", 3600L),
                        new Event("Kally", "./prod?id=1", 3800L),
                        new Event("Kally", "./prod?id=2", 4200L)
                )
                // 有序流的watermark生成
//                .assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps()
//                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
//                            @Override
//                            public long extractTimestamp(Event element, long recordTimestamp) {
//                                return element.timestamp;
//                            }
//                        }));
                // 乱序流的watermark生成
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );

        // 4. 执行环境
        env.execute();
    }


}
