package com.program.chapter5.datasource;


import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Random;

/**
 * @author : kaisy
 * @date : 2022/5/6 10:34
 * @Description : customer defined source
 */
public class SourceCustomTest {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // 2. 读取自定义数据源数据
//        DataStreamSource<Event> customStream = env.addSource(new ClickSource());
        DataStreamSource<Integer> customStream = env.addSource(new ParallelCustomerSource()).setParallelism(2);


        // 3. 打印数据
//        customStream.print("cus");
        customStream.print();

        // 4. 执行环境
        env.execute();
    }

    public static class ParallelCustomerSource implements ParallelSourceFunction<Integer>  {
        private Boolean runFlag = true;
        private Random random = new Random();

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            while(runFlag){
                ctx.collect(random.nextInt());
            }
        }

        @Override
        public void cancel() {
            runFlag = false;
        }
    }
}
