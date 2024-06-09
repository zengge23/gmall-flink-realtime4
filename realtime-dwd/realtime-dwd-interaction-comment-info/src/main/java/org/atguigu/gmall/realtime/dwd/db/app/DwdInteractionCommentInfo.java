package org.atguigu.gmall.realtime.dwd.db.app;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.atguigu.gmall.realtime.common.base.BaseSQLApp;
import org.atguigu.gmall.realtime.common.util.SQLUtil;

import static org.atguigu.gmall.realtime.common.constant.Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO;

public class DwdInteractionCommentInfo extends BaseSQLApp {
    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv, String ckAndGroupId) {
        createTopicDb(ckAndGroupId, tEnv);

        createBaseDic(tEnv);

        Table commentInfo = tEnv.sqlQuery("select\n" +
                "    data['id'] `id` ,\n" +
                "    data['user_id'] `user_id` ,\n" +
                "    data['nick_name'] `nick_name` ,\n" +
                "    data['head_img'] `head_img` ,\n" +
                "    data['sku_id'] `sku_id` ,\n" +
                "    data['spu_id'] `spu_id` ,\n" +
                "    data['order_id'] `order_id` ,\n" +
                "    data['appraise'] `appraise` ,\n" +
                "    data['comment_txt'] `comment_txt` ,\n" +
                "    data['create_time'] `create_time` ,\n" +
                "    data['operate_time'] `operate_time` ,\n" +
                "    proc_time\n" +
                "from topic_db\n" +
                "where `database` = `gmall`\n" +
                "and `table` = `comment_info`\n" +
                "and `type` = `insert`");

        tEnv.createTemporaryView("comment_info", commentInfo);

        Table joinTable = tEnv.sqlQuery("select\n" +
                "    id,\n" +
                "    user_id,\n" +
                "    nick_name,\n" +
                "    head_img,\n" +
                "    sku_id,\n" +
                "    spu_id,\n" +
                "    order_id,\n" +
                "    appraise appraise_code,\n" +
                "    info.dic_name appraise_name,\n" +
                "    comment_txt,\n" +
                "    create_time,\n" +
                "    operate_time\n" +
                "from comment_info C\n" +
                "join base_dic FOR SYSTEM_TIME AS c.proc_time as b on c.appraise = b.rowkey");


        tEnv.executeSql("create table " + TOPIC_DWD_INTERACTION_COMMENT_INFO + " (\n" +
                "    id,\n" +
                "    user_id,\n" +
                "    nick_name,\n" +
                "    sku_id,\n" +
                "    spu_id,\n" +
                "    order_id,\n" +
                "    appraise_code,\n" +
                "    appraise_name,\n" +
                "    comment_txt,\n" +
                "    create_time,\n" +
                "    operate_time\n" +
                ")" + SQLUtil.getKafkaDDLSink(TOPIC_DWD_INTERACTION_COMMENT_INFO));

        joinTable.insertInto(TOPIC_DWD_INTERACTION_COMMENT_INFO).execute();
    }
}


























