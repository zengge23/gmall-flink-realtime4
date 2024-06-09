package org.atguigu.gmall.realtime.dwd.db.app;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.atguigu.gmall.realtime.common.base.BaseSQLApp;
import org.atguigu.gmall.realtime.common.util.SQLUtil;

import static org.atguigu.gmall.realtime.common.constant.Constant.TOPIC_DWD_TRADE_CART_ADD;

public class DwdTradeCartAdd extends BaseSQLApp {

    public static void main(String[] args) {
        new DwdTradeCartAdd().start(10013, 4, "dwd_trade_cart_add");
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv, String ckAndGroupId) {

        createTopicDb(ckAndGroupId, tEnv);

        Table cartAddTable = tEnv.sqlQuery("select\n" +
                "    `data`['id'] id,\n" +
                "    `data`['user_id'] user_id,\n" +
                "    `data`['sku_id'] sku_id,\n" +
                "    `data`['cart_price'] cart_price,\n" +
                "    if(`type`='insert',`data`['sku_num'],cast(cast(`data`['sku_num'] as bigint) - cast(`old`['sku_num'] as bigint) as STRING)) sku_num,\n" +
                "    `data`['sku_name'] sku_name,\n" +
                "    `data`['is_checked'] is_checked,\n" +
                "    `data`['create_time'] create_time,\n" +
                "    `data`['operate_time'] operate_time,\n" +
                "    `data`['is_ordered'] is_ordered,\n" +
                "    `data`['order_time'] order_time,\n" +
                "    `data`['source_type'] source_type,\n" +
                "    `data`['source_id' ] source_id,\n" +
                "     ts\n" +
                "from topic_db\n" +
                "where `database`='gmall'\n" +
                "and `table`='cart_info'\n" +
                "and (`type`='insert' or (`type`='update' and `old`['sku_num'] is not null and cast(`data`['sku_num'] as bigint) > cast(`old`['sku_num'] as bigint)))");

//        tEnv.createTemporaryView("cart_add", cartAddTable);


        tEnv.executeSql("create table " + TOPIC_DWD_TRADE_CART_ADD + "(\n" +
                "    id STRING,\n" +
                "    user_id STRING,\n" +
                "    sku_id STRING,\n" +
                "    cart_price STRING,\n" +
                "    sku_num STRING,\n" +
                "    sku_name STRING,\n" +
                "    is_checked STRING,\n" +
                "    create_time STRING,\n" +
                "    operate_time STRING,\n" +
                "    is_ordered STRING,\n" +
                "    order_time STRING,\n" +
                "    source_type STRING,\n" +
                "    source_id STRING,\n" +
                "    ts BIGINT\n" +
                ") " + SQLUtil.getKafkaDDLSink(TOPIC_DWD_TRADE_CART_ADD));

        cartAddTable.insertInto(TOPIC_DWD_TRADE_CART_ADD).execute();
    }

}
