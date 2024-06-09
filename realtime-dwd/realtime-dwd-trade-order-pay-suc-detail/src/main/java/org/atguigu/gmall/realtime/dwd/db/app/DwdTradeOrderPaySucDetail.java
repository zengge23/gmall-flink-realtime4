package org.atguigu.gmall.realtime.dwd.db.app;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.atguigu.gmall.realtime.common.base.BaseSQLApp;
import org.atguigu.gmall.realtime.common.util.SQLUtil;

import static org.atguigu.gmall.realtime.common.constant.Constant.TOPIC_DWD_TRADE_ORDER_DETAIL;
import static org.atguigu.gmall.realtime.common.constant.Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS;

public class DwdTradeOrderPaySucDetail extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeOrderPaySucDetail().start(10016, 4, "dwd_trade_order_pay_suc_detail");
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv, String ckAndGroupId) {
        createTopicDb(ckAndGroupId, tEnv);

        Table paymentInfoTable = tEnv.sqlQuery("select\n" +
                "    `data`['id'] id,\n" +
                "    `data`['order_id'] order_id,\n" +
                "    `data`['user_id'] user_id,\n" +
                "    `data`['payment_type'] payment_type,\n" +
                "    `data`['total_amount'] total_amount,\n" +
                "    `data`['callback_time'] callback_time,\n" +
                "    row_time,\n" +
                "    proc_time,\n" +
                "    ts\n" +
                "from topic_db\n" +
                "where `database`='gmall'\n" +
                "and `table`='payment_info'\n" +
                "and `type`='update'\n" +
                "and `old`['payment_status'] is not null\n" +
                "and `data`['payment_status']='1602'");
        tEnv.createTemporaryView("payment_info_table", paymentInfoTable);
//        tEnv.sqlQuery("select * from payment_info_table").execute().print();

        tEnv.executeSql("CREATE TABLE order_detail (\n" +
                "    id STRING,\n" +
                "    order_id STRING,\n" +
                "    sku_id STRING,\n" +
                "    province_id STRING,\n" +
                "    user_id STRING,\n" +
                "    activity_id STRING,\n" +
                "    activity_rule_id STRING,\n" +
                "  coupon_id  STRING,\n" +
                "    sku_name STRING,\n" +
                "    order_price STRING,\n" +
                "    sku_num STRING,\n" +
                "    create_time STRING,\n" +
                "    split_total_amount STRING,\n" +
                "    split_activity_amount STRING,\n" +
                "    split_coupon_amount STRING,\n" +
                "    ts bigint,\n" +
                "    row_time as TO_TIMESTAMP_LTZ(ts,3)" +
                ",\n" +
                "    WATERMARK FOR row_time AS row_time - INTERVAL '15' SECOND\n" +
                ") " + SQLUtil.getKafkaDDLSource(TOPIC_DWD_TRADE_ORDER_DETAIL, ckAndGroupId));

//        tEnv.sqlQuery("select * from order_detail").execute().print();

        createBaseDic(tEnv);

        Table payOrderTable = tEnv.sqlQuery("select\n" +
                "    od.id,\n" +
                "    p.order_id,\n" +
                "    p.user_id,\n" +
                "    payment_type,\n" +
                "    callback_time payment_time,\n" +
                "    sku_id,\n" +
                "    province_id,\n" +
                "    activity_id,\n" +
                "    activity_rule_id,\n" +
                "    coupon_id,\n" +
                "    sku_name,\n" +
                "    order_price,\n" +
                "    sku_num,\n" +
                "    split_total_amount,\n" +
                "    split_activity_amount,\n" +
                "    split_coupon_amount,\n" +
                "    p.proc_time,\n" +
                "    p.ts\n" +
                "from payment_info_table p,order_detail od\n" +
                "where p.order_id = od.order_id\n" +
                "and p.row_time BETWEEN od.row_time - INTERVAL '15' MINUTE AND od.row_time + INTERVAL '5' SECOND");

        tEnv.createTemporaryView("pay_order_table", payOrderTable);

//        tEnv.sqlQuery("select * from pay_order_table").execute().print();

        Table resultTable = tEnv.sqlQuery("select\n" +
                "    id,\n" +
                "    order_id,\n" +
                "    user_id,\n" +
                "    payment_type payment_type_code,\n" +
                "    info.dic_name payment_type_name,\n" +
                "    payment_time,\n" +
                "    sku_id,\n" +
                "    province_id,\n" +
                "    activity_id,\n" +
                "    activity_rule_id,\n" +
                "    coupon_id,\n" +
                "    sku_name,\n" +
                "    order_price,\n" +
                "    sku_num,\n" +
                "    split_total_amount,\n" +
                "    split_activity_amount,\n" +
                "    split_coupon_amount,\n" +
                "    ts\n" +
                "from pay_order_table p\n" +
                "left join base_dic FOR SYSTEM_TIME AS OF p.proc_time AS b on p.payment_type = b.rowkey");

//        tEnv.createTemporaryView("resultTable", resultTable);
//
//        tEnv.executeSql("select * from resultTable").print();

        tEnv.executeSql("create table " + TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS + " (\n" +
                "    id STRING,\n" +
                "    order_id STRING,\n" +
                "    user_id STRING,\n" +
                "    payment_type_code STRING,\n" +
                "    payment_type_name STRING,\n" +
                "    payment_time STRING,\n" +
                "    sku_id STRING,\n" +
                "    province_id STRING,\n" +
                "    activity_id STRING,\n" +
                "    activity_rule_id STRING,\n" +
                "    coupon_id STRING,\n" +
                "    sku_name STRING,\n" +
                "    order_price STRING,\n" +
                "    sku_num STRING,\n" +
                "    split_total_amount STRING,\n" +
                "    split_activity_amount STRING,\n" +
                "    split_coupon_amount STRING,\n" +
                "    ts BIGINT,\n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                ")" + SQLUtil.getUpsertKafkaSQL(TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS));

        resultTable.insertInto(TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS).execute();
    }
}
