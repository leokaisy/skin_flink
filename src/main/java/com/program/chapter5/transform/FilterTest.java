package com.program.chapter5.transform;

import com.program.chapter5.datasource.Event;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author : kaisy
 * @date : 2022/5/6 14:55
 * @Description : filter
 */
public class FilterTest {
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
//        ds.map(x -> x.user.equals("kally")).print("3");
        // 3.1 使用自定义类，实现MapFunction接口
        SingleOutputStreamOperator<Event> result1 = ds.filter(new MyFilter());
        // 3.2 使用匿名类实现MapFunction接口
        SingleOutputStreamOperator<Event> result2 = ds.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event e) throws Exception {
                return e.user.equals("Felisa");
            }
        });

        // 4. 数据打印
        result1.print("1");
        result2.print("2");

        // 5. 执行
        env.execute();
    }
    // 实现自定义FilterFunction
    public static class MyFilter implements FilterFunction<Event> {

        @Override
        public boolean filter(Event event) throws Exception {
            return event.user.equals("Kally");
        }
    }
}
