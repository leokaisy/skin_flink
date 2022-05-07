package com.program.chapter5.datasource;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

/**
 * @author : kaisy
 * @date : 2022/5/6 10:35
 * @Description : source function
 */
public class ClickSource implements SourceFunction<Event> {
    // 声明一个标志位
    private Boolean runFlag = true;

    @Override
    public void run(SourceContext ctx) throws Exception {
        // 随机生成数据
        Random random = new Random();
        // 定义字段选取的数据集
        String[] users = {"Mary","Bob","Alice","Kally","Christina"};
        String[] urls = {"./home", "./cart", "./ fav", "./prod?id=100", "./prod?id=10", "./pop.org", "./sink"};

        // 循环生成数据
        while(runFlag){
            ctx.collect(new Event(users[random.nextInt(users.length)],
                    urls[random.nextInt(urls.length)],
                    Calendar.getInstance().getTimeInMillis())
            );
            Thread.sleep(1000L);
        }
    }

    @Override
    public void cancel() {
        runFlag = false;
    }
}
