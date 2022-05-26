package com.program.chapter7;

import com.program.chapter5.datasource.ClickSource;
import com.program.chapter5.datasource.Event;
import com.program.chapter6.window.UrlCountViewTest;
import com.program.chapter6.window.UrlViewCount;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hdfs.server.common.JspHelper;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;

/**
 * @author : kaisy
 * @date : 2022/5/24 15:17
 * @Description : top n
 */
public class TopNExample {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 获取数据源
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));

        // 3. 按照url分组，统计窗口内每个url的访问量
        SingleOutputStreamOperator<UrlViewCount> urlCountStream = stream.keyBy(data -> data.url)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5))) // 定义滑动窗口
                .aggregate(new UrlCountViewTest.UrlViewCountAgg(), new UrlCountViewTest.UrlCountResult());

        urlCountStream.print("url count");

        // 3.2 对于同一窗口统计出的访问量，进行收集和排序
        urlCountStream.keyBy(UrlViewCount::getWindowEnd)
                .process(new TopNProcessResult(2))
                .print();

        // 4. 执行环境
        env.execute();
    }

    // 实现自定义的KeyedProcessFunction
    public static class TopNProcessResult extends KeyedProcessFunction<Long , UrlViewCount , String>{
        // 定义一个属性
        private int n;
        // 定义列表状态
        private ListState<UrlViewCount> urlViewCountListState;

        public TopNProcessResult(int n) {
            this.n = n;
        }

        // 在环境中获取转态


        @Override
        public void open(Configuration parameters) throws Exception {
            urlViewCountListState = getRuntimeContext().getListState(
                    new ListStateDescriptor<UrlViewCount>("url-count-list", Types.POJO(UrlViewCount.class))
            );
        }

        @Override
        public void processElement(UrlViewCount value, KeyedProcessFunction<Long, UrlViewCount, String>.Context ctx, Collector<String> out) throws Exception {
            // 将数据保存到状态中
            urlViewCountListState.add(value);
            // 注册windowEnd + 1ms的定时器
            ctx.timerService().registerEventTimeTimer(ctx.getCurrentKey() + 1);

        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, UrlViewCount, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            ArrayList<UrlViewCount> urlViewCountArr = new ArrayList<>();
            for (UrlViewCount urlViewCount :urlViewCountListState.get()) {
                urlViewCountArr.add(urlViewCount);
            }
            urlViewCountArr.sort(new Comparator<UrlViewCount>() {
                @Override
                public int compare(UrlViewCount o1, UrlViewCount o2) {
                    return o2.getCount().intValue() - o1.getCount().intValue();
                }
            });

            // 包装信息，打印输出
            StringBuilder result = new StringBuilder();
            result.append("-----------------------------\n");
            result.append("窗口结束时间： ").append(new Timestamp(ctx.getCurrentKey())).append("\n");

            // 取list前两个，包装信息输出
            for (int i = 0 ; i < 2 ; i++){
                UrlViewCount currTuple = urlViewCountArr.get(i);
                String info = "No. " + (i + 1) + "  " +
                        "url: " + currTuple.getUrl() + "  " +
                        "访问量: " + currTuple.getCount() + "\n";
                result.append(info);
            }
            result.append("-----------------------------\n");

            out.collect(result.toString());
        }


    }


}
