package org.atguigu.gmall.realtime.dwd.db.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.atguigu.gmall.realtime.common.base.BaseAPP;
import org.atguigu.gmall.realtime.common.bean.TableProcessDwd;
import org.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import org.atguigu.gmall.realtime.common.util.FlinkSourceUtil;
import org.atguigu.gmall.realtime.common.util.JdbcUtil;

import java.sql.Connection;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.atguigu.gmall.realtime.common.constant.Constant.*;

public class DwdBaseDb extends BaseAPP {

    public static void main(String[] args) {
        new DwdBaseDb().start(10019, 4, "dwd_base_db", TOPIC_DB);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
//        stream.print();

        SingleOutputStreamOperator<JSONObject> jsonObjStream = stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("异常数据 >>> " + s);
                }
            }
        });

        DataStreamSource<String> tableProcessDwd = env.fromSource(FlinkSourceUtil.getMySqlSource(PROCESS_DATABASE, PROCESS_DWD_TABLE_NAME), WatermarkStrategy.noWatermarks(), "table_process_dwd").setParallelism(1);

        SingleOutputStreamOperator<TableProcessDwd> processDwdStream = tableProcessDwd.flatMap(new FlatMapFunction<String, TableProcessDwd>() {
            @Override
            public void flatMap(String s, Collector<TableProcessDwd> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    String op = jsonObject.getString("op");
                    TableProcessDwd processDwd;
                    if ("d".equals(op)) {

                        processDwd = jsonObject.getObject("before", TableProcessDwd.class);
                    } else {
                        processDwd = jsonObject.getObject("after", TableProcessDwd.class);
                    }
                    processDwd.setOp(op);
                    collector.collect(processDwd);
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("数据转换异常 >>> " + s);
                }
            }
        }).setParallelism(1);

        MapStateDescriptor<String, TableProcessDwd> processState = new MapStateDescriptor<>("process_state", String.class, TableProcessDwd.class);
        BroadcastStream<TableProcessDwd> broadcastStream = processDwdStream.broadcast(processState);

        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> processStream = jsonObjStream.connect(broadcastStream).process(new BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>() {
            Map<String, TableProcessDwd> map = new HashMap<String, TableProcessDwd>();

            @Override
            public void open(Configuration parameters) throws Exception {
                Connection mysqlConnection = JdbcUtil.getMysqlConnection();
                List<TableProcessDwd> tableProcessDwds = JdbcUtil.queryList(mysqlConnection, "select * from gmall_config_realtime4.table_process_dwd", TableProcessDwd.class, true);
                for (TableProcessDwd tableProcessDwd : tableProcessDwds) {
                    map.put(tableProcessDwd.getSourceTable() + ":" + tableProcessDwd.getSourceType(), tableProcessDwd);
                }
            }

            @Override
            public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.ReadOnlyContext readOnlyContext, Collector<Tuple2<JSONObject, TableProcessDwd>> collector) throws Exception {
                String table = jsonObject.getString("table");
                String type = jsonObject.getString("type");
                String key = table + ":" + type;
                ReadOnlyBroadcastState<String, TableProcessDwd> broadcastState = readOnlyContext.getBroadcastState(processState);
                TableProcessDwd processDwd = broadcastState.get(key);
                if (processDwd == null) {
                    processDwd = map.get(key);
                }
                if (processDwd != null) {
                    collector.collect(Tuple2.of(jsonObject, processDwd));
                }
            }

            @Override
            public void processBroadcastElement(TableProcessDwd tableProcessDwd, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.Context context, Collector<Tuple2<JSONObject, TableProcessDwd>> collector) throws Exception {
                BroadcastState<String, TableProcessDwd> broadcastState = context.getBroadcastState(processState);
                String op = tableProcessDwd.getOp();
                String key = tableProcessDwd.getSourceTable() + ":" + tableProcessDwd.getSourceType();
                if ("d".equals(op)) {
                    broadcastState.remove(key);
                    map.remove(key);
                } else {
                    broadcastState.put(key, tableProcessDwd);
                }
            }
        }).setParallelism(1);

        SingleOutputStreamOperator<JSONObject> dataStream = processStream.map(new MapFunction<Tuple2<JSONObject, TableProcessDwd>, JSONObject>() {
            @Override
            public JSONObject map(Tuple2<JSONObject, TableProcessDwd> value) throws Exception {
                JSONObject jsonObject = value.f0;
                TableProcessDwd tableProcessDwd1 = value.f1;
                JSONObject data = jsonObject.getJSONObject("data");
                List<String> columns = Arrays.asList(tableProcessDwd1.getSinkColumns().split(","));
                data.keySet().removeIf(json -> !columns.contains(json));
                data.put("sink_table", tableProcessDwd1.getSinkTable());
                return data;
            }
        });

        dataStream.print(" >>>>>  ");

        dataStream.sinkTo(FlinkSinkUtil.getKafkaSink());
    }
}

















