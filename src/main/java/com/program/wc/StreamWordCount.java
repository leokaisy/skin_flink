package com.program.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
<<<<<<< HEAD
=======
import org.apache.flink.api.java.utils.ParameterTool;
>>>>>>> fa9d9a5 (flink transformation learning)
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author : kaisy
 * @date : 2022/4/26 16:23
 * @Description : unbounded stream wc
 */
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        // 1. 创建一个流式的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

<<<<<<< HEAD
        // 2. 读取文本流
        DataStreamSource<String> lineDataStream = env.socketTextStream("localhost", 7777);
=======
        // 从参数中提取主机名和端口号
        ParameterTool paraTool = ParameterTool.fromArgs(args);
        String host = paraTool.get("host");
        int port = paraTool.getInt("port");

        // 2. 读取文本流
        DataStreamSource<String> lineDataStream = env.socketTextStream(host, port);
>>>>>>> fa9d9a5 (flink transformation learning)

        // 3. 转换计算
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOneTuple = lineDataStream.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
                    String[] words = line.split(" ");
                    for (String word : words) {
                        out.collect(Tuple2.of(word, 1L));
                    }
                })
                .returns(Types.TUPLE(Types.STRING, Types.LONG));

        // 4. 分组
        KeyedStream<Tuple2<String, Long>, String> wordAndOneKeyedStream = wordAndOneTuple.keyBy(data -> data.f0);

        // 5. 求和
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = wordAndOneKeyedStream.sum(1);

        // 6. 打印
        sum.print();

        // 7. 启动执行
        env.execute();

    }

}
