package com.program.func;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

/**
 * @author : kaisy
 * @date : 2022/4/25 16:39
 * @Description : user defined deserialization
 */
public class CustomerDeserializationSchema implements DebeziumDeserializationSchema<String> {
    /**
     *{
     *     "db":""
     *     "tableName":""
     *     "before":""
     *     "after":""
     *     "op":""
     *}
     */
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        // 创建JSON对象用于封装结果数据
        JSONObject result = new JSONObject();

        // 获取库名&表名
        String topic = sourceRecord.topic();
        String[] fields = topic.split("\\.");
        result.put("db",fields[1]);
        result.put("tableName",fields[2]);

        // 获取value消息
        Struct value = (Struct) sourceRecord.value();
        // 获取before消息
        JSONObject beforeJson = getDataJson(value, "before");
        result.put("before",beforeJson);

        // 获取after消息
        JSONObject afterJson = getDataJson(value, "after");
        result.put("after",afterJson);

        // 获取操作类型
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        result.put("op", operation);

        // 输出数据
        collector.collect(result.toJSONString());
    }

    private JSONObject getDataJson(Struct value, String data) {
        Struct dataStruct = value.getStruct(data);
        JSONObject dataJson = new JSONObject();
        if(dataStruct != null){
            //获取列信息
            Schema schema = dataStruct.schema();
            List<Field> fieldList = schema.fields();
            //将before列名数据推到json中
            for (Field field : fieldList){
                dataJson.put(field.name(), dataStruct.get(field));
            }
        }
        return dataJson;
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
