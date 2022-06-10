package com.program.chapter9;

import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import scala.Tuple2;

import java.time.Duration;

/**
 * @author : kaisy
 * @date : 2022/6/2 16:05
 * @Description : broadcast stream state
 */
public class BehaviorPatternDetectExample {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 读取数据流
        // 2.1 用户的行为数据流
        SingleOutputStreamOperator<Action> actionStream = env.fromElements(
                    new Action("Alice","login"),
                    new Action("Alice","pay"),
                    new Action("Bob","login"),
                    new Action("Bob","order"),
                    new Action("kally","login"),
                    new Action("kally","view")
                );

        // 2.2 行为模式流，基于它构建广播流
        DataStreamSource<Pattern> patternStream = env.fromElements(
                new Pattern("login", "pay"),
                new Pattern("login", "order"),
                new Pattern("login", "view")
        );


        // 2.3 定义广播状态描述器
        MapStateDescriptor<Void, Pattern> patternDescriptor = new MapStateDescriptor<>("pattern", Types.VOID, Types.POJO(Pattern.class));
        BroadcastStream<Pattern> broadcastStream = patternStream.broadcast(patternDescriptor);

        //连接两条流进行数据处理
        SingleOutputStreamOperator<Tuple2<String,Pattern>> matches = actionStream.keyBy(data -> data.userId)
                .connect(broadcastStream)
                .process(new PatternDetector());

        matches.print("broadcast: ");

        // 4. 执行环境
        env.execute();
    }

    // 定义用户行为事件和模式的POJO类
    public static class Action {
        public String userId;
        public String action;

        public Action() {
        }

        public Action(String userId, String action) {
            this.userId = userId;
            this.action = action;
        }

        @Override
        public String toString() {
            return "Action{" +
                    "userId='" + userId + '\'' +
                    ", action='" + action + '\'' +
                    '}';
        }
    }

    public static class Pattern{
        public String action1;
        public String action2;

        public Pattern() {
        }

        public Pattern(String action1, String action2) {
            this.action1 = action1;
            this.action2 = action2;
        }

        @Override
        public String toString() {
            return "Pattern{" +
                    "action1='" + action1 + '\'' +
                    ", action2='" + action2 + '\'' +
                    '}';
        }
    }

    // 实现自定义的KeyedBroadcastProcessFunction
    public static class PatternDetector extends KeyedBroadcastProcessFunction<String , Action ,Pattern , Tuple2<String , Pattern>>{
        // 定义一个keyedState，保存用户的上一次行为
        ValueState<String> preActionState;

        @Override
        public void open(Configuration parameters) throws Exception {
            preActionState = getRuntimeContext().getState(new ValueStateDescriptor<String>("last-action", String.class));
        }

        @Override
        public void processElement(Action value, ReadOnlyContext ctx, Collector<Tuple2<String, Pattern>> out) throws Exception {
            // 从广播状态中获取匹配模式
            ReadOnlyBroadcastState<Void, Pattern> patternState = ctx.getBroadcastState(new MapStateDescriptor<>("pattern", Types.VOID, Types.POJO(Pattern.class)));
            Pattern pattern = patternState.get(null);

            // 获取用户上一次的行为
            String preAction = preActionState.value();

            // 判断是否匹配
            if(pattern != null && preAction != null){
                if (pattern.action1.equals(preAction) && pattern.action2.equals(value.action)) {
                    out.collect(new Tuple2<>(ctx.getCurrentKey(),pattern));
                }
            }

            // 更新状态
            preActionState.update(value.action);
        }

        @Override
        public void processBroadcastElement(Pattern value, Context ctx, Collector<Tuple2<String, Pattern>> out) throws Exception {
            // 从上文中获取广播状态，并用当前数据更新状态
            BroadcastState<Void, Pattern> patternState = ctx.getBroadcastState(new MapStateDescriptor<>("pattern", Types.VOID, Types.POJO(Pattern.class)));
            // 更新状态
            patternState.put(null, value);

        }
    }
}
