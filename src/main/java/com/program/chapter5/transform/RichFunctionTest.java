package com.program.chapter5.transform;

import com.program.chapter5.datasource.Event;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author : kaisy
 * @date : 2022/5/7 9:54
 * @Description : rich function
 */
public class RichFunctionTest {
    public static void main(String[] args) throws Exception{
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 从元素读取数据
        DataStreamSource<Event> ds = env.fromElements(
                new Event("Kally", "./tmt", 3000L),
                new Event("Felisa", "./pob.org", 4000L),
                new Event("Alice", "./home", 5000L)
        );

        // 3. 数据处理，将url的长度输出
        ds.map(new MyRichMapper()).print("rich");

        // 4. 执行
        env.execute();
    }

    // 自定义一个mapFunction的富函数类 继承自AbstractRichFunction,实现open 和 close方法（生命周期管理）
    public static class MyRichMapper extends RichMapFunction<Event,Integer> {
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            System.out.println("open生命周期被调用，" + getRuntimeContext().getIndexOfThisSubtask() + "号任务启动！");
        }

        @Override
        public Integer map(Event value) throws Exception {
            return value.url.length();
        }

        @Override
        public void close() throws Exception {
            super.close();
            System.out.println("close生命周期被调用，" + getRuntimeContext().getIndexOfThisSubtask() + "号任务结束！");
        }
    }
}
