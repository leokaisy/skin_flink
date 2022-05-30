package com.program.chapter8;

import com.program.chapter5.datasource.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author : kaisy
 * @date : 2022/5/27 16:22
 * @Description : stream connect
 */
public class ConnectTest {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2.1 获取第一个数据源
        DataStreamSource<Integer> stream1 = env.fromElements(1, 2, 3);
        // 2.2 获取第二个数据源
        DataStreamSource<Long> stream2 = env.fromElements(4L, 5L, 6L);

        // 3.3 合并连接两个流
        stream2.connect(stream1)
                .map(new CoMapFunction<Long, Integer, String>() {
                    @Override
                    public String map1(Long value) throws Exception {
                        return "Long: " + value;
                    }

                    @Override
                    public String map2(Integer value) throws Exception {
                        return "Integer: " + value;
                    }
                })
                .print("connect: ");



        // 4. 执行环境
        env.execute();
    }
}
