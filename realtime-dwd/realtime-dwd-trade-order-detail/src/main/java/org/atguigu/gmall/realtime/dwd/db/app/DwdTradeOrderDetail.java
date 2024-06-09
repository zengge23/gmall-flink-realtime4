package org.atguigu.gmall.realtime.dwd.db.app;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.atguigu.gmall.realtime.common.base.BaseSQLApp;
import org.atguigu.gmall.realtime.common.util.SQLUtil;

import java.time.Duration;

import static org.atguigu.gmall.realtime.common.constant.Constant.TOPIC_DWD_TRADE_ORDER_DETAIL;

public class DwdTradeOrderDetail extends BaseSQLApp {

    public static void main(String[] args) {
        new DwdTradeOrderDetail().start(10024, 4, "dwd_trade_order_detail");
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv, String ckAndGroupId) {
        //设置过期时间,在 FlinkSQL 中使用 join 一定要添加状态的存活时间
        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5L));

        createTopicDb(ckAndGroupId, tEnv);

        Table orderTable = tEnv.sqlQuery("select\n" +
                "    `data`['id'] id,\n" +
                "    `data`['order_id'] order_id,\n" +
                "    `data`['sku_id'] sku_id,\n" +
                "    `data`['sku_name'] sku_name,\n" +
                "    `data`['order_price'] order_price,\n" +
                "    `data`['sku_num'] sku_num,\n" +
                "    `data`['create_time'] create_time,\n" +
                "    `data`['split_total_amount'] split_total_amount,\n" +
                "    `data`['split_activity_amount'] split_activity_amount,\n" +
                "    `data`['split_coupon_amount'] split_coupon_amount,\n" +
                "    ts\n" +
                "from topic_db\n" +
                "where `database`='gmall'\n" +
                "and `table`='order_detail'\n" +
                "and `type`='insert'");
        tEnv.createTemporaryView("order_detail", orderTable);

        Table orderInfoTable = tEnv.sqlQuery("select\n" +
                "    `data`['id'] id,\n" +
                "    `data`['user_id'] user_id,\n" +
                "    `data`['province_id'] province_id\n" +
                "from topic_db\n" +
                "where `database`='gmall'\n" +
                "and `table`='order_info'\n" +
                "and `type`='insert'");
        tEnv.createTemporaryView("order_info", orderInfoTable);

        Table orderDetailActivityTable = tEnv.sqlQuery("select\n" +
                "    `data`['order_detail_id'] id,\n" +
                "    `data`['activity_id'] activity_id,\n" +
                "    `data`['activity_rule_id'] activity_rule_id\n" +
                "from topic_db\n" +
                "where `database`='gmall'\n" +
                "and `table`='order_dateil_activity'\n" +
                "and `type`='insert'");
        tEnv.createTemporaryView("order_dateil_activity", orderDetailActivityTable);

        Table orderDetailCouponTable = tEnv.sqlQuery("select\n" +
                "    `data`['order_detail_id'] id,\n" +
                "    `data`['coupon_id'] coupon_id\n" +
                "from topic_db\n" +
                "where `database`='gmall'\n" +
                "and `table`='order_dateil_coupon'\n" +
                "and `type`='insert'");
        tEnv.createTemporaryView("order_dateil_coupon", orderDetailCouponTable);

        Table joinTable = tEnv.sqlQuery("select\n" +
                "    od.id,\n" +
                "    order_id,\n" +
                "    sku_id,\n" +
                "    province_id,\n" +
                "    user_id,\n" +
                "    activity_id,\n" +
                "    activity_rule_id,\n" +
                "    sku_name,\n" +
                "    order_price,\n" +
                "    sku_num,\n" +
                "    create_time,\n" +
                "    split_total_amount,\n" +
                "    split_activity_amount,\n" +
                "    split_coupon_amount,\n" +
                "    ts\n" +
                "from order_detail od\n" +
                "join order_info oi on od.order_id = oi.id\n" +
                "left join order_dateil_activity oda on oda.id = od.id\n" +
                "left join order_dateil_coupon odc on odc.id = od.id");

        //输出到 Kafka
        //一旦使用了 left join 会产生撤回流，此时若要输出数据到 kafka 不能使用一般的 kafka sink ,必须使用 upsert kafka
        tEnv.executeSql("CREATE TABLE " + TOPIC_DWD_TRADE_ORDER_DETAIL + " (\n" +
                "    id STRING,\n" +
                "    order_id STRING,\n" +
                "    sku_id STRING,\n" +
                "    province_id STRING,\n" +
                "    user_id STRING,\n" +
                "    activity_id STRING,\n" +
                "    activity_rule_id STRING,\n" +
                "    sku_name STRING,\n" +
                "    order_price STRING,\n" +
                "    sku_num STRING,\n" +
                "    create_time STRING,\n" +
                "    split_total_amount STRING,\n" +
                "    split_activity_amount STRING,\n" +
                "    split_coupon_amount STRING,\n" +
                "    ts bigint,\n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                ") " + SQLUtil.getUpsertKafkaSQL(TOPIC_DWD_TRADE_ORDER_DETAIL));

        joinTable.insertInto(TOPIC_DWD_TRADE_ORDER_DETAIL).execute();
    }
}
