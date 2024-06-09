package org.atguigu.gmall.realtime.common.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.hadoop.hbase.client.Connection;
import org.atguigu.gmall.realtime.common.bean.TableProcessDim;
import org.atguigu.gmall.realtime.common.util.HBaseUtil;
import org.atguigu.gmall.realtime.common.util.RedisUtil;
import redis.clients.jedis.Jedis;

import java.io.IOException;

import static org.atguigu.gmall.realtime.common.constant.Constant.HBASE_NAMESPACE;

public class DimHBaseSinkFunc extends RichSinkFunction<Tuple2<JSONObject, TableProcessDim>> {
    //    new RichSinkFunction<Tuple2<JSONObject, TableProcessDim>>() {
    Connection connection;
    Jedis jedis;

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = HBaseUtil.getConnection();
        jedis = new Jedis();
    }

    @Override
    public void close() throws Exception {
        HBaseUtil.closeConnection(connection);
        RedisUtil.closeJedis(jedis);
    }

    @Override
    public void invoke(Tuple2<JSONObject, TableProcessDim> value, SinkFunction.Context context) throws Exception {
        JSONObject jsonObject = value.f0;
        TableProcessDim dim = value.f1;
        String type = jsonObject.getString("type");
        JSONObject data = jsonObject.getJSONObject("data");
        if ("delete".equals(type)) {
            delete(data, dim);
        } else {
            put(data, dim);
        }

        if ("delete".equals(type) || "update".equals(type)) {
            jedis.del(RedisUtil.getKey(dim.getSinkTable(),data.getString(dim.getSinkRowKey())));
        }
    }

    private void put(JSONObject data, TableProcessDim dim) {
        String sinkTable = dim.getSinkTable();
        String sinkRowKeyName = dim.getSinkRowKey();
        String sinkRowKeyValue = data.getString(sinkRowKeyName);
        String sinkFamily = dim.getSinkFamily();
        try {
            HBaseUtil.putCells(connection, HBASE_NAMESPACE, sinkTable, sinkRowKeyValue, sinkFamily, data);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void delete(JSONObject data, TableProcessDim dim) {
        String sinkTable = dim.getSinkTable();
        String sinkRowKeyName = dim.getSinkRowKey();
        String sinkRowKeyValue = data.getString(sinkRowKeyName);
        try {
            HBaseUtil.deleteCells(connection, HBASE_NAMESPACE, sinkTable, sinkRowKeyValue);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
//    }
}
