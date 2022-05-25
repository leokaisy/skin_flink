package com.program.flinkcdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author : kaisy
 * @date : 2022/5/17 9:09
 * @Description :
 */
public class SyncTest {
    public static void main(String[] args) {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        tableEnv.getConfig().getConfiguration().setString("pipeline.name", "Adidas_ListMember_Sync");

//        // 导入营销列表数据源
//        String listMemberBaseSourceDDL =
//                "CREATE TABLE `listMemberBase_lgh` (\n" +
//                        "\t`EntityId`       VARCHAR(100),\n" +
//                        "\t`ListId`         VARCHAR(100),\n" +
//                        "\t`CreatedOn`      TIMESTAMP(3),\n" +
//                        "\t`CreatedBy`      VARCHAR(50),\n" +
//                        "\t`ModifiedOn`     TIMESTAMP(3),\n" +
//                        "\t`ModifiedBy`     VARCHAR(50)\n" +
//                        " ) WITH (\n" +
//                        "        'connector' = 'sqlserver-cdc',\n" +
//                        "        'hostname' = 'dev-crm-nlb-1a-0012664966e44e79.elb.cn-north-1.amazonaws.com.cn',\n" +
//                        "        'port' = '5023',\n" +
//                        "        'username' = 'testcrmsql',\n" +
//                        "        'password' = 'TestC@#mSql@32',\n" +
//                        "        'database-name' = 'AdidasCRM_MSCRM',\n" +
//                        "        'schema-name' = 'dbo',\n" +
//                        "        'table-name' = 'ListMemberBase',\n" +
//                        "        'scan.startup.mode' = 'initial'\n" +
//                        ")";
//
//        // 营销列表 Sink DDL
//        String rulesCustomerSinkDDL =
//                "CREATE TABLE `t_rules_customer` (\n" +
//                        "    `rule_effective_customer_code`    varchar(64)     ,\n"+
//                        "    `customer_code`                   varchar(64)     ,\n"+
//                        "    `created_by`                      varchar(32)     ,\n"+
//                        "    `created_time`                    TIMESTAMP       ,\n"+
//                        "    `updated_by`                      varchar(32)     ,\n"+
//                        "    `updated_time`                    TIMESTAMP       ,\n"+
//                        "    `is_deleted`                      INT             ,\n"+
//                        "PRIMARY KEY (`rule_effective_customer_code`) NOT ENFORCED \n"+
//                        ") WITH (\n" +
//                        "    'connector' = 'jdbc',\n" +
//                        "    'url' = 'jdbc:mysql://localhost:3306/test?characterEncoding=UTF-8',\n" +
//                        "    'table-name' = 't_rules_customer',\n" +
//                        "    'username' = 'root',\n" +
//                        "    'password' = '123456'\n" +
//                        ")";
//
//        // 数据转换 写入 营销列表 sql
//        String rulesCustomer = "INSERT INTO t_rules_customer (" +
//                " `rule_effective_customer_code`    \n" +
//                ",`customer_code`                   \n" +
//                ",`created_by`                      \n" +
//                ",`created_time`                    \n" +
//                ",`updated_by`                      \n" +
//                ",`updated_time`                    \n" +
//                ",`is_deleted`                      \n)"+
//                "SELECT                             \n" +
//                "    `ListId`         AS `rule_effective_customer_code`     \n" +
//                "   ,`EntityId`       AS `customer_code`                    \n" +
//                "   ,`CreatedBy`      AS `created_by`                       \n" +
//                "   ,`CreatedOn`      AS `created_time`                     \n" +
//                "   ,`ModifiedBy`     AS `updated_by`                       \n" +
//                "   ,`ModifiedOn`     AS `updated_time`                     \n" +
//                "   ,0                AS `is_deleted`                       \n" +
//                "FROM\n" +
//                "listMemberBase_lgh";
//
//        // 执行sql
//        tableEnv.executeSql(listMemberBaseSourceDDL);
//        tableEnv.executeSql(rulesCustomerSinkDDL);
//        tableEnv.executeSql(rulesCustomer);


        String user =
                "CREATE TABLE `test_table` (\n" +
                        "\t`user`          VARCHAR(100),\n" +
                        "\t`url`           VARCHAR(100)\n" +
                        " ) WITH (\n" +
                        " 'connector' = 'mysql-cdc', " +
                        " 'scan.startup.mode' = 'initial', " +
                        " 'hostname' = 'localhost', " +
                        " 'port' = '3306', " +
                        " 'username' = 'root', " +
                        " 'password' = '123456', " +
                        " 'database-name' = 'test', " +
                        " 'table-name' = 'clicks' " +
                        ")";

        // 执行sql
        tableEnv.executeSql(user);


        Table table = tableEnv.sqlQuery("select * from test_table");
        tableEnv.toRetractStream(table, Row.class).print();
    }
}
