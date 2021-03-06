package com.program.chapter9;

import com.program.chapter5.datasource.ClickSource;
import com.program.chapter5.datasource.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * @author : kaisy
 * @date : 2022/6/2 14:55
 * @Description : state split
 */
public class BufferingSinkExample {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 设置检查点
        env.enableCheckpointing(30000L);
        // 设置状态后端
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        //设置检查点存储路径
//        env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage("hdfs://name"));
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointTimeout(6000L);                           // 设置检查点保存的超时时间，超时没完成就会被丢弃掉
        checkpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE); // 设置检查点模式
        checkpointConfig.setMinPauseBetweenCheckpoints(500L);                   // 设置下一次检查点保存的最小间隔
        checkpointConfig.setMaxConcurrentCheckpoints(1);                        // 运行中的检查点最多可以有多少个（并发度）；设置了最小间隔，这个并行度必须为1
        // 开启检查点的外部持久化，而且默认在作业失败的时候不会自动清理，如果想释放空间需要自己手工清理
        // DELETE_ON_CANCELLATION：在作业取消的时候会自动删除外部检查点，但是如果是作业失败退出，则会保留检查点。
        // RETAIN_ON_CANCELLATION：作业取消的时候也会保留外部检查点
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
        checkpointConfig.enableUnalignedCheckpoints();                          // 启用不对其的检查点保存

        // 2. 添加数据源
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy. <Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));
        stream.print("input");


        // 3. 数据处理
        stream.addSink(new BufferingSink(10));

        // 4. 执行环境
        env.execute();
    }


    // 自定义实现SinkFunction
    public static class BufferingSink implements SinkFunction<Event>, CheckpointedFunction {
        // 定义当前类的属性
        private final int threshold;
        // 定义列表
        private List<Event> bufferedElements;

        public BufferingSink(int threshold) {
            this.threshold = threshold;
            this.bufferedElements = new ArrayList<>();
        }

        // 定义一个算子状态
        private ListState<Event> checkpointedState;

        @Override
        public void invoke(Event value, Context context) throws Exception {

            bufferedElements.add(value);  // 缓存到列表
            // 如果达到阈值，就批量写入
            if (bufferedElements.size() == threshold) {
                // 用打印到控制台模拟写入外部系统
                for (Event element : bufferedElements)
                    System.out.println(element);

                System.out.println("==========输出完毕==========");
                bufferedElements.clear();
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            // 清空状态
            checkpointedState.clear();

            // 对状态进行持久化,复制缓存的列表到列表状态
            for (Event element : bufferedElements)
                checkpointedState.add(element);

        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            // 定义算子状态
            ListStateDescriptor<Event> descriptor = new ListStateDescriptor<>("buffered-element", Event.class);

            checkpointedState = context.getOperatorStateStore().getListState(descriptor);

            // 如果从故障恢复，需要将liststate中的数据复制到列表中
            if (context.isRestored()) {
                for (Event element : checkpointedState.get()) {
                    bufferedElements.add(element);
                }
            }
        }
    }
}
