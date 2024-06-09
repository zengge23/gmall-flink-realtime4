package org.atguigu.gmall.realtime.dwd.db.app;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.atguigu.gmall.realtime.common.base.BaseSQLApp;
import org.atguigu.gmall.realtime.common.util.SQLUtil;

import static org.atguigu.gmall.realtime.common.constant.Constant.TOPIC_DWD_TRADE_ORDER_REFUND;

public class DwdTradeOrderRefund extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeOrderRefund().start(10017, 1, TOPIC_DWD_TRADE_ORDER_REFUND);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv, String ckAndGroupId) {
        createTopicDb(ckAndGroupId, tableEnv);

        //过滤退单表数据 order_refund_info   insert
        Table orderRefundInfo = tableEnv.sqlQuery(
                "select \n" +
                        "    data['id'] id,\n" +
                        "    data['user_id'] user_id,\n" +
                        "    data['order_id'] order_id,\n" +
                        "    data['sku_id'] sku_id,\n" +
                        "    data['refund_type'] refund_type,\n" +
                        "    data['refund_num'] refund_num,\n" +
                        "    data['refund_amount'] refund_amount,\n" +
                        "    data['refund_reason_type'] refund_reason_type,\n" +
                        "    data['refund_reason_txt'] refund_reason_txt,\n" +
                        "    data['create_time'] create_time,\n" +
                        "    proc_time as pt,\n" +
                        "    row_time as rt,\n" +
                        "    ts\n" +
                        "    from topic_db\n" +
                        "    where `database`='gmall'\n" +
                        "    and `table`='order_refund_info'\n" +
                        "    and `type`='insert'"
        );

        tableEnv.createTemporaryView("order_refund_info", orderRefundInfo);

//        tableEnv.executeSql("select * from order_refund_info").print();


        // 3. 过滤订单表中的退单数据: order_info  update
        Table orderInfo = tableEnv.sqlQuery(
                "select " +
                        "data['id'] id," +
                        "data['province_id'] province_id," +
                        "`old` " +
                        "from topic_db " +
                        "where `database`='gmall' " +
                        "and `table`='order_info' " +
                        "and `type`='update'" +
                        "and `old`['order_status'] is not null " +
                        "and `data`['order_status']='1005' ");
        tableEnv.createTemporaryView("orderInfo", orderInfo);

//        tableEnv.executeSql("select * from orderInfo").print();

        createBaseDic(tableEnv);

        // 4. join: 普通的和 lookup join
        Table result = tableEnv.sqlQuery(
                "select\n" +
                        "    ri.id,\n" +
                        "    ri.user_id,\n" +
                        "    ri.order_id,\n" +
                        "    ri.sku_id,\n" +
                        "    oi.province_id,\n" +
                        "    date_format(ri.create_time, 'yyyy-MM-dd') date_id,\n" +
                        "    ri.create_time,\n" +
                        "    ri.refund_type,\n" +
                        "    dic1.info.dic_name,\n" +
                        "    ri.refund_reason_type,\n" +
                        "    dic2.info.dic_name,\n" +
                        "    ri.refund_reason_txt,\n" +
                        "    ri.refund_num,\n" +
                        "    ri.refund_amount,\n" +
//                        "    ri.rt,\n" +
                        "    ri.ts\n" +
                        "from\n" +
                        "    order_refund_info ri\n" +
                        "    join orderInfo oi on ri.order_id = oi.id\n" +
                        "    join base_dic for system_time as of ri.pt as dic1 on ri.refund_type = dic1.rowkey\n" +
                        "    join base_dic for system_time as of ri.pt as dic2 on ri.refund_reason_type = dic2.rowkey");

        tableEnv.createTemporaryView("result_info", result);

//        tableEnv.executeSql("select * from result_info").print();

        // 5. 写出到 kafka
        tableEnv.executeSql(
                "create table dwd_trade_order_refund(" +
                        "id string," +
                        "user_id string," +
                        "order_id string," +
                        "sku_id string," +
                        "province_id string," +
                        "date_id string," +
                        "create_time string," +
                        "refund_type_code string," +
                        "refund_type_name string," +
                        "refund_reason_type_code string," +
                        "refund_reason_type_name string," +
                        "refund_reason_txt string," +
                        "refund_num string," +
                        "refund_amount string," +
//                        "rt string, " +
                        "ts bigint " +
                        ")" + SQLUtil.getKafkaDDLSink(TOPIC_DWD_TRADE_ORDER_REFUND));

        result.insertInto(TOPIC_DWD_TRADE_ORDER_REFUND).execute();

    }
}
