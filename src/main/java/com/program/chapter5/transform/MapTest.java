package com.program.chapter5.transform;

import com.program.chapter5.datasource.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author : kaisy
 * @date : 2022/5/6 11:39
 * @Description : map
 */
public class MapTest {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 从元素读取数据
        DataStreamSource<Event> ds = env.fromElements(
                new Event("Kally", "./tmt", 3000L),
                new Event("Felisa", "./pob.org", 4000L)
        );

        // 3. 进行转换计算
//        SingleOutputStreamOperator<String> user = ds.map(x -> x.user);
        // 3.1 使用自定义类，实现MapFunction接口
        SingleOutputStreamOperator<String> result1 = ds.map(new MyMapper());
        // 3.2 使用匿名类实现MapFunction接口
        SingleOutputStreamOperator<String> result2 = ds.map(new MapFunction<Event, String>() {
            @Override
            public String map(Event e) throws Exception {
                return e.user;
            }
        });

        // 4. 数据打印
        result1.print("1");
        result2.print("2");

        // 5. 执行
        env.execute();
    }

    // 自定义MapFunction
    public static class MyMapper implements MapFunction<Event,String> {

        @Override
        public String map(Event event) throws Exception {
            return event.user;
        }
    }
}
