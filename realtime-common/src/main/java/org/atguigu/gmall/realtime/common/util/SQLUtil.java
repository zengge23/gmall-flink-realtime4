package org.atguigu.gmall.realtime.common.util;

import org.atguigu.gmall.realtime.common.constant.Constant;

import static org.atguigu.gmall.realtime.common.constant.Constant.*;

public class SQLUtil {

    public static String getKafkaDDLSource(String topic, String groupId) {
        return "WITH(" +
                "  'connector' = 'kafka'," +
                "  'properties.group.id' = '" + groupId + "'," +
                "  'topic' = '" + topic + "'," +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "'," +
                "  'scan.startup.mode' = 'latest-offset'," +
//                "  'json.ignore-parse-errors' = 'true'," + // 当 json 解析失败的时候,忽略这条数据
                "  'format' = 'json' " +
                ")";
    }

    public static String getKafkaDDLSink(String topic) {
        return "WITH(" +
                "  'connector' = 'kafka'," +
                "  'topic' = '" + topic + "'," +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "'," +
                "  'format' = 'json' " +
                ")";
    }

    public static String getKafkaTopicDb(String groupId) {
        return "CREATE TABLE topic_db(\n" +
                "    `database` STRING,\n" +
                "    `table` STRING,\n" +
                "    `type` STRING,\n" +
                "    `ts` bigint,\n" +
                "    `data` map<STRING,STRING>,\n" +
                "    `old` map<STRING,STRING>,\n" +
                "    proc_time as PROCTIME(),\n" +
                "    row_time as TO_TIMESTAMP_LTZ(ts,3),\n" +
                "    WATERMARK FOR row_time AS row_time - INTERVAL '15' SECOND\n" +
                ") " + getKafkaDDLSource(TOPIC_DB, groupId);
    }

    /**
     * 获取 upsert kafka 的连接 创建表格的语句最后一定要声明主键
     * @param topicName
     * @return
     */
    public static String getUpsertKafkaSQL(String topicName){
        return "WITH(" +
                "  'connector' = 'upsert-kafka'," +
                "  'topic' = '" + topicName + "'," +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "'," +
                "  'key.format' = 'json', " +
                "  'value.format' = 'json' " +
                ")";
    }

    public static String getDorisSinkSQL(String tableName){
        return "with(\n" +
                "    'connector' = 'doris',\n" +
                "    'fenodes' = '" + FENODES + "',\n" +
                "    'table.identifier' = '" + DORIS_DATABASE + "." + tableName + "',\n" +
                "    'username' = '" + DORIS_USERNAME + "',\n" +
                "    'password' = '" + DORIS_PASSWORD + "',\n" +
                "    'sink.label-prefix' = 'doris_label" + System.currentTimeMillis() +"'\n" +
                ")";
    }

}




















