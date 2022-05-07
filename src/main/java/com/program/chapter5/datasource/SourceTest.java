package com.program.chapter5.datasource;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.ArrayList;
import java.util.Properties;

/**
 * @author : kaisy
 * @date : 2022/5/5 18:00
 * @Description : flink source
 */
public class SourceTest {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        // 2. 读取数据
        // 2.1 从文件中读取数据
        DataStreamSource<String> stream1 = env.readTextFile("input/clicks.txt");

        // 2.2 从集合中读取数据
        ArrayList<Integer> nums = new ArrayList<>();
        nums.add(2);
        nums.add(5);
        DataStreamSource<Integer> numStream = env.fromCollection(nums);

        ArrayList<Event> events = new ArrayList<>();
        events.add(new Event("Mary", "./home", 1000L));
        events.add(new Event("Bob", "./cart", 2000L));
        DataStreamSource<Event> stream2 = env.fromCollection(events);

        // 2.3 从元素读取数据
        DataStreamSource<Event> stream3 = env.fromElements(
                new Event("Kally", "./tmt", 3000L),
                new Event("Felisa", "./pob.org", 4000L)
        );

        // 2.4 从socket文本流中读取
        DataStreamSource<String> stream4 = env.socketTextStream("hadoop01", 7777);

        // 2.5 自定义kafka数据源
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop101:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");
        DataStreamSource<String> kafkaStream = env.addSource(new FlinkKafkaConsumer<String>(
                "clicks",
                new SimpleStringSchema(),
                properties
        ));

        // 3. 打印数据
        stream1.print("1");
        numStream.print("nums");
        stream2.print("2");
        stream3.print("3");
        stream4.print("4");
        kafkaStream.print("kafka");

        // 4. 执行
        env.execute();
    }
}
