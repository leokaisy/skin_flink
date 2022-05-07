package com.program.chapter5.sink;

import com.program.chapter5.datasource.Event;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author : kaisy
 * @date : 2022/5/7 14:46
 * @Description : mysql sink
 */
public class SinkToMySql {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 从元素读取数据
        DataStreamSource<Event> ds = env.fromElements(new Event("Mary","./cart",1000L),
                new Event("Kally", "./tmt", 2000L),
                new Event("Felisa", "./pob.org", 3000L),
                new Event("Kally", "./vab", 3300L),
                new Event("Kally", "./home", 3500L),
                new Event("Felisa", "./home", 3600L),
                new Event("Kally", "./prod?id=1", 3800L),
                new Event("Kally", "./prod?id=2", 4200L)
        );

        // 3. 将数据写入mysql
        ds.addSink(JdbcSink.sink(
                "INSERT INTO clicks (user , url) VALUES (? , ?)",
                (statement , event) -> {
                    statement.setString(1,event.user);
                    statement.setString(2,event.url);
                },
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://localhost:3306/test")
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("123456")
                        .build()
        ));

        // 4. 执行环境
        env.execute();
    }
}
