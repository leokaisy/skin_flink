package com.program.flinkcdc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author : kaisy
 * @date : 2022/4/25 15:07
 * @Description : Flink SQL
 */
public class FlinkSQLCDC {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2.  使用FlinkSql DDL模式构建cdc表
        tableEnv.executeSql("CREATE TABLE user_info_defined ( " +
                "user_id STRING primary key, " +
                "user_name STRING, " +
                "gender STRING" +
                ") WITH (" +
                " 'connector' = 'mysql-cdc', " +
                " 'scan.startup.mode' = 'latest-offset', " +
                " 'hostname' = 'localhost', " +
                " 'port' = '3306', " +
                " 'username' = 'root', " +
                " 'password' = '123456', " +
                " 'database-name' = 'kaisy', " +
                " 'table-name' = 'user_info' " +
                ")");

        // 3. 查询数据并转换为流输出
        // 3.1 查询数据
        Table table = tableEnv.sqlQuery("select * from user_info_defined");
        // 3.2 转换流
        DataStream<Tuple2<Boolean, Row>> dataStream = tableEnv.toRetractStream(table, Row.class);
        // 3.3 数据打印
        dataStream.print();

        // 4. 启动程序
        env.execute("FlinkSQLCDC");
    }
}
