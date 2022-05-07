package com.program.chapter5.transform;

import com.program.chapter5.datasource.Event;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * @author : kaisy
 * @date : 2022/5/7 10:14
 * @Description : repartition
 */
public class PartitionTest {
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

        // 3.分区
        // 3.1 随机分区
        ds.shuffle().print().setParallelism(4);

        // 3.2 轮询分区(若充分去前后线程不一致，重分区时默认调用轮询分区)
        ds.rebalance().print().setParallelism(4);

        // 3.3 rescale 重缩放分区
        env.addSource(new RichParallelSourceFunction<Integer>() {
            @Override
            public void run(SourceContext<Integer> ctx) throws Exception {
                for (int i = 1; i <= 8; i++) {
                    // 将奇偶数分别发放到0号和1号并行分区
                    if(i % 2 == getRuntimeContext().getIndexOfThisSubtask()){
                        ctx.collect(i);
                    }
                }
            }

            @Override
            public void cancel() {}
        }).setParallelism(2)
                .rescale()
                .print()
                .setParallelism(4);

        // 3.4 广播分区(每条数据都在每个分区进行广播)
        ds.broadcast().print().setParallelism(4);

        // 3.5 全局分区(将所有数据全分配到一个分区)
        ds.global().print().setParallelism(4);

        // 3.6 自定义重分区
        env.fromElements(1, 2, 3, 4, 5, 6, 7, 8)
                .partitionCustom(new Partitioner<Integer>() {
                    @Override
                    public int partition(Integer key, int numPartitions) {
                        return key % 2;
                    }
                },new KeySelector<Integer, Integer>(){
                    @Override
                    public Integer getKey(Integer value) throws Exception {
                        return value;
                    }
                })
                .print()
                .setParallelism(4);


        // 4. 执行环境
        env.execute();
    }
}
