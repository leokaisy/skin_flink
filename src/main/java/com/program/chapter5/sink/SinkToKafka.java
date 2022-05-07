package com.program.chapter5.sink;

import com.program.chapter5.datasource.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * @author : kaisy
 * @date : 2022/5/7 13:42
 * @Description : kafka sink
 */
public class SinkToKafka {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 从kafka中读取数据
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop101:9092");

        DataStreamSource<String> kafkaStream = env.addSource(new FlinkKafkaConsumer<String>(
                "clicks",
                new SimpleStringSchema(),
                properties
        ));

        // 3. 用flink进行转换处理
        SingleOutputStreamOperator<String> kafkaResult = kafkaStream.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                String[] fileds = value.split(",");
                return new Event(fileds[0].trim(), fileds[1].trim(), Long.valueOf(fileds[2].trim())).toString();
            }
        });


        // 4. 将数据写入kafka另一个topic
        kafkaResult.addSink(new FlinkKafkaProducer<String>("hadoop102:9092",
                "events",
                new SimpleStringSchema())
        );

        // 5. 执行环境
        env.execute();

    }
}
