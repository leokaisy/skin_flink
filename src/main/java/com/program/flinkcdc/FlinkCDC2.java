package com.program.flinkcdc;

import com.program.func.CustomerDeserializationSchema;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author : kaisy
 * @date : 2022/4/25 13:41
 * @Description :
 */
public class FlinkCDC2 {
     public static void main(String[] args) throws Exception {
          // 1. 创建flink执行环境
          StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
          env.setParallelism(1);

          // 从参数中提取主机名，端口号，数据库和表名字
          ParameterTool tool = ParameterTool.fromArgs(args);
          String host = tool.get("host");
          int port = tool.getInt("port");
          String dataBase =  tool.get("dataBase");
          String table =  tool.get("table");

//          // 1.1 开启ck
//          env.enableCheckpointing(5000);
//          env.getCheckpointConfig().setCheckpointTimeout(10000);
//          env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//          env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//
//          env.setStateBackend(new FsStateBackend("hdfs://192.168.50.120:8020/cdc-test/ck"));

          // 2. 通过flinkCDC构建SourceFunction
          DebeziumSourceFunction<String> sourceFunction = MySqlSource.<String>builder()
                  .hostname(host)
                  .port(port)
                  .username("root")
                  .password("123456")
                  .databaseList(dataBase)
                  .tableList(table)
                  .serverTimeZone("UTC")
                  .deserializer(new CustomerDeserializationSchema())
                  .startupOptions(StartupOptions.initial())
                  .build();
          // 3. 连接source
          DataStreamSource<String> dataStreamSource = env.addSource(sourceFunction);

          // 4. 数据打印
          dataStreamSource.print();

          // 5. 启动任务
          env.execute("FlinkCDC");
     }
}
