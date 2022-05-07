package com.program.chapter5.sink;

import com.program.chapter5.datasource.ClickSource;
import com.program.chapter5.datasource.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @author : kaisy
 * @date : 2022/5/7 14:00
 * @Description : Redis sink
 */
public class SinkToReidis {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 从元素读取数据
        DataStreamSource<Event> ds = env.addSource(new ClickSource());

        // 3. 将数据写入Redis
        // 3.1 创建一个jedis连接配置
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder()
                .setHost("hadoop101")
                .build();
        // 3.2 将数据写入Redis(实时监控的情况下非常好用)
        ds.addSink(new RedisSink<>(config, new MyRedisMapper()));

        // 4. 执行环境
        env.execute();
    }

    // 自定义类实现RedisMapper接口
    public static class MyRedisMapper implements RedisMapper<Event>{

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET,"clicks");
        }

        @Override
        public String getKeyFromData(Event data) {
            return data.user;
        }

        @Override
        public String getValueFromData(Event data) {
            return data.url;
        }
    }

}
