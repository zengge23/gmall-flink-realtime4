package org.atguigu.gmall.realtime.dim.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;
import org.atguigu.gmall.realtime.common.base.BaseAPP;
import org.atguigu.gmall.realtime.common.bean.TableProcessDim;
import org.atguigu.gmall.realtime.common.function.DimHBaseSinkFunc;
import org.atguigu.gmall.realtime.common.util.FlinkSourceUtil;
import org.atguigu.gmall.realtime.common.util.HBaseUtil;
import org.atguigu.gmall.realtime.common.util.JdbcUtil;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.atguigu.gmall.realtime.common.constant.Constant.*;

public class DimApp extends BaseAPP {

    public static void main(String[] args) {
        new DimApp().start(10001, 4, "dim_app", "topic_db");
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
//        SingleOutputStreamOperator<JSONObject> streamOperator = stream.filter(new FilterFunction<String>() {
//            @Override
//            public boolean filter(String s) throws Exception {
//                boolean flag = false;
//                try {
//                    JSONObject jsonObject = JSON.parseObject(s);
//                    String database = jsonObject.getString("database");
//                    String type = jsonObject.getString("type");
//                    JSONObject data = jsonObject.getJSONObject("data");
//                    if ("gmall".equals(database) && !"bootstarp".equals(type) && !"bootstarp-complate".equals(type) && data != null && !data.isEmpty()) {
//                        flag = true;
//                    }
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//                return flag;
//            }
//        }).map(JSON::parseObject);


        SingleOutputStreamOperator<JSONObject> filterDS = stream.flatMap(new RichFlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                String database = jsonObject.getString("database");
                String type = jsonObject.getString("type");
                JSONObject data = jsonObject.getJSONObject("data");
                if ("gmall".equals(database) && !"bootstarp".equals(type) && !"bootstarp-complate".equals(type) && data != null && !data.isEmpty()) {
                    collector.collect(jsonObject);
                }
            }
        });

//        filterDS.print();

        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMySqlSource(PROCESS_DATABASE, PROCESS_DIM_TABLE_NAME);

        DataStreamSource<String> mysqlSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source").setParallelism(1);
        mysqlSource.print();


        SingleOutputStreamOperator<TableProcessDim> createTableStream = mysqlSource.flatMap(new RichFlatMapFunction<String, TableProcessDim>() {
            Connection connection;

            @Override
            public void open(Configuration parameters) throws Exception {
                connection = HBaseUtil.getConnection();
            }

            @Override
            public void close() throws Exception {
                HBaseUtil.closeConnection(connection);
            }

            @Override
            public void flatMap(String s, Collector<TableProcessDim> collector) throws Exception {
                //使用读取的配置表数据到 HBase 中创建与之对应的表格
                try {
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    String op = jsonObject.getString("op");
                    TableProcessDim dim;
                    if ("d".equals(op)) {
                        dim = jsonObject.getObject("before", TableProcessDim.class);
                        deleteTable(dim);
                    } else if ("c".equals(op) || "r".equals(op)) {
                        dim = jsonObject.getObject("after", TableProcessDim.class);
                        createTable(dim);
                    } else {
                        dim = jsonObject.getObject("after", TableProcessDim.class);
                        deleteTable(dim);
                        createTable(dim);
                    }
                    dim.setOp(op);
                    collector.collect(dim);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            private void deleteTable(TableProcessDim dim) {
                try {
                    HBaseUtil.dropTable(connection, HBASE_NAMESPACE, dim.getSinkTable());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            private void createTable(TableProcessDim dim) {
                String sinkFamily = dim.getSinkFamily();
                String[] split = sinkFamily.split(",");
                try {
                    HBaseUtil.createTable(connection, HBASE_NAMESPACE, dim.getSinkTable(), split);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }).setParallelism(1);

        createTableStream.print();
        //广播状态的 Key 用于判断是否是维度表 Value用于补充信息写出到 HBase
        MapStateDescriptor<String, TableProcessDim> broadcastState = new MapStateDescriptor<>("broadcast_state", String.class, TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastStream = createTableStream.broadcast(broadcastState);

        BroadcastConnectedStream<JSONObject, TableProcessDim> connectedStream = filterDS.connect(broadcastStream);

        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimStream = connectedStream.process(new BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>() {
            public HashMap<String, TableProcessDim> hashMap;

            @Override
            public void open(Configuration parameters) throws Exception {
                java.sql.Connection mysqlConnection = JdbcUtil.getMysqlConnection();
                List<TableProcessDim> tableProcessDims = JdbcUtil.queryList(mysqlConnection, "select * from gmall_config_realtime4.table_process_dim", TableProcessDim.class, true);
                hashMap = new HashMap<>();
                for (TableProcessDim tableProcessDim : tableProcessDims) {
                    tableProcessDim.setOp("r");
                    hashMap.put(tableProcessDim.getSourceTable(), tableProcessDim);
                }
                JdbcUtil.closeConnection(mysqlConnection);
            }

            /**
             * 处理主流数据
             * @param jsonObject
             * @param readOnlyContext
             * @param collector
             * @throws Exception
             */
            @Override
            public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.ReadOnlyContext readOnlyContext, Collector<Tuple2<JSONObject, TableProcessDim>> collector) throws Exception {
                ReadOnlyBroadcastState<String, TableProcessDim> tableBroadcastState = readOnlyContext.getBroadcastState(broadcastState);
                String table = jsonObject.getString("table");
                TableProcessDim dim = tableBroadcastState.get(table);
                if (dim == null) {
                    dim = hashMap.get(table);
                }
                if (dim != null) {
                    collector.collect(Tuple2.of(jsonObject, dim));
                }
            }

            /**
             * 处理广播流数据
             * @param tableProcessDim
             * @param context
             * @param collector
             * @throws Exception
             */
            @Override
            public void processBroadcastElement(TableProcessDim tableProcessDim, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.Context context, Collector<Tuple2<JSONObject, TableProcessDim>> collector) throws Exception {
                BroadcastState<String, TableProcessDim> tableBroadcastState = context.getBroadcastState(broadcastState);
                String op = tableProcessDim.getOp();
                if ("d".equals(op)) {
                    tableBroadcastState.remove(tableProcessDim.getSinkTable());
                    hashMap.remove(tableProcessDim.getSourceTable());
                } else {
                    tableBroadcastState.put(tableProcessDim.getSinkTable(), tableProcessDim);
                }
            }
        }).setParallelism(1);

        dimStream.print();

        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> filterColumnStream = dimStream.map(new MapFunction<Tuple2<JSONObject, TableProcessDim>, Tuple2<JSONObject, TableProcessDim>>() {
            @Override
            public Tuple2<JSONObject, TableProcessDim> map(Tuple2<JSONObject, TableProcessDim> value) throws Exception {
                JSONObject jsonObj = value.f0;
                TableProcessDim dim = value.f1;
                String sinkColumns = dim.getSinkColumns();
                List<String> columns = Arrays.asList(sinkColumns.split(","));
                JSONObject data = jsonObj.getJSONObject("data");
                data.keySet().removeIf(key -> !columns.contains(key));
                return value;
            }
        });
        filterColumnStream.print();
        filterColumnStream.addSink(new DimHBaseSinkFunc());
    }
}









































