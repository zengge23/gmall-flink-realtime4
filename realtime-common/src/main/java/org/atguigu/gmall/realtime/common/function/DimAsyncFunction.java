package org.atguigu.gmall.realtime.common.function;

import com.alibaba.fastjson.JSONObject;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.atguigu.gmall.realtime.common.util.HBaseUtil;
import org.atguigu.gmall.realtime.common.util.RedisUtil;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.atguigu.gmall.realtime.common.constant.Constant.HBASE_NAMESPACE;

public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T,T> implements DimJoinFunction<T>{

    StatefulRedisConnection<String, String> redisAsyncConnection;
    AsyncConnection asyncConnection;

    @Override
    public void close() throws Exception {
        RedisUtil.closeAsyncJedis(redisAsyncConnection);
        HBaseUtil.closeAsyncConnection(asyncConnection);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        redisAsyncConnection = RedisUtil.getRedisAsyncConnection();
        asyncConnection = HBaseUtil.getAsyncConnection();
    }

    @Override
    public void asyncInvoke(T t, ResultFuture<T> resultFuture) throws Exception {
        String tableName = getTableName();
        String rowKey = getId(t);
        String key = RedisUtil.getKey(tableName, rowKey);
        CompletableFuture.supplyAsync(new Supplier<String>() {
            @Override
            public String get() {
                RedisFuture<String> dimSkuInfoFuture = redisAsyncConnection.async().get(key);
                String dimInfo = null;
                try {
                    dimInfo = dimSkuInfoFuture.get();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return dimInfo;
            }
        }).thenApplyAsync(new Function<String, JSONObject>() {
            @Override
            public JSONObject apply(String dimInfo) {
                JSONObject dimJsonObj = null;
                if (dimInfo == null || dimInfo.isEmpty()) {
                    try {
                        dimJsonObj = HBaseUtil.getAsyncCells(asyncConnection, HBASE_NAMESPACE, tableName, rowKey);
                        redisAsyncConnection.async().setex(key, 24 * 60 * 60, dimJsonObj.toJSONString());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } else {
                    dimJsonObj = JSONObject.parseObject(dimInfo);
                }
                return dimJsonObj;
            }
        }).thenAccept(new Consumer<JSONObject>() {
            @Override
            public void accept(JSONObject jsonObject) {
                if (jsonObject == null || jsonObject.isEmpty()) {
                    System.out.println("关联的维度信息异常: " + tableName + ":" + key);
                } else {
                    join(t, jsonObject);
                }
                resultFuture.complete(Collections.singletonList(t));
            }
        });
    }
}
