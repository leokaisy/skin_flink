package com.program.chapter11;

import com.program.chapter5.datasource.ClickSource;
import com.program.chapter5.datasource.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author : kaisy
 * @date : 2022/6/13 15:07
 * @Description : table api 简单案例
 */
public class SimpleTableExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 添加数据源
        SingleOutputStreamOperator<Event> envStream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));

        // 创建表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 将DataStream转换成Table
        Table eventTable = tableEnv.fromDataStream(envStream);

        // 直接写sql进行转换
        Table resultTable1 = tableEnv.sqlQuery("select user , url from " + eventTable);

        // 基于table直接转换
        Table resultTable2 = eventTable.select($("user"), $("url"))
                .where($("user").isEqual("Alice"));


        tableEnv.toDataStream(resultTable1).print("result1");
        tableEnv.toDataStream(resultTable2).print("result2");

        env.execute();


    }
}
