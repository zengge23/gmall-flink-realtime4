package org.atguigu.gmall.realtime.common.util;

import com.alibaba.fastjson.JSONObject;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.SimpleStringSerializer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.atguigu.gmall.realtime.common.constant.Constant;

import javax.annotation.Nullable;
import java.util.Properties;
import java.util.Random;

import static org.atguigu.gmall.realtime.common.constant.Constant.*;

public class FlinkSinkUtil {

    public static KafkaSink<String> getKafkaSink(String topic) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.<String>builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("atguigu-" + topic + new Random().nextLong())
                .setProperty("transaction.timeout.ms", 15 * 60 * 1000 + "")
                .build();
    }

    public static KafkaSink<JSONObject> getKafkaSink() {
        return KafkaSink.<JSONObject>builder()
                .setBootstrapServers(KAFKA_BROKERS)
                .setRecordSerializer(new KafkaRecordSerializationSchema<JSONObject>() {
                    @Nullable
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObject, KafkaSinkContext kafkaSinkContext, Long aLong) {
                        String sinkTable = jsonObject.getString("sink_table");
                        jsonObject.remove("sink_table");
                        return new ProducerRecord<byte[], byte[]>(sinkTable, Bytes.toBytes(jsonObject.toJSONString()));
                    }
                }).setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("atguigu-base_db" + System.currentTimeMillis())
                .setProperty("transaction.timeout.ms", 15 * 60 * 1000 + "").build();
    }

//    public static DorisSink<String> getDorisSink(String tableName) {
//
//        Properties properties = new Properties();
//        // 上游是json写入时，需要开启配置
//        properties.setProperty("format", "json");
//        properties.setProperty("read_json_by_line", "true");
//
//        return DorisSink.<String>builder()
//                .setDorisReadOptions(DorisReadOptions.builder().build())
//                .setDorisExecutionOptions(
//                        DorisExecutionOptions.builder()
//                                .setLabelPrefix("label-doris" + System.currentTimeMillis()) //streamload label prefix
//                                .setDeletable(false)
//                                .setStreamLoadProp(properties).build()
//                )
//                .setSerializer(new SimpleStringSerializer()) //serialize according to string
//                .setDorisOptions(DorisOptions.builder()
//                        .setFenodes(Constant.FENODES)
//                        .setTableIdentifier(Constant.DORIS_DATABASE + "." + tableName)
//                        .setUsername(Constant.DORIS_USERNAME)
//                        .setPassword(Constant.DORIS_PASSWORD)
//                        .build())
//                .build();
//    }

    public static DorisSink<String> getDorisSink(String tableName) {
        Properties properties = new Properties();
        // 上游是 json 写入时，需要开启配置
        properties.setProperty("format", "json");
        properties.setProperty("read_json_by_line", "true");

        return DorisSink.<String>builder()
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(
                        DorisExecutionOptions.builder()
                                .setLabelPrefix("label-doris" + System.currentTimeMillis()) //streamload label prefix
                                .setDeletable(false)
                                .setStreamLoadProp(properties).build()
                )
                .setSerializer(new SimpleStringSerializer()) //serialize according to string
                .setDorisOptions(DorisOptions.builder()
                        .setFenodes(FENODES)
                        .setTableIdentifier(DORIS_DATABASE + "." + tableName)
                        .setUsername(DORIS_USERNAME)
                        .setPassword(DORIS_PASSWORD)
                        .build())
                .build();
    }

}










