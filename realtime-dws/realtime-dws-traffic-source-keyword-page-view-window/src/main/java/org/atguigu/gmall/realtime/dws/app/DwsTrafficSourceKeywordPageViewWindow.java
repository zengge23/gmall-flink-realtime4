package org.atguigu.gmall.realtime.dws.app;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.atguigu.gmall.realtime.common.base.BaseSQLApp;
import org.atguigu.gmall.realtime.common.util.SQLUtil;
import org.atguigu.gmall.realtime.dws.function.KwSplit;

import static org.atguigu.gmall.realtime.common.constant.Constant.DWS_TRAFFIC_SOURCE_KEYWORD_PAGE_VIEW_WINDOW;
import static org.atguigu.gmall.realtime.common.constant.Constant.TOPIC_DWD_TRAFFIC_PAGE;

public class DwsTrafficSourceKeywordPageViewWindow extends BaseSQLApp {

    public static void main(String[] args) {
        new DwsTrafficSourceKeywordPageViewWindow().start(10021, 1, "dws_traffic_source_keyword_page_view_window");
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv, String ckAndGroupId) {
        tableEnv.executeSql(" create table page_info(\n" +
                "    `common` map<STRING,STRING>,\n" +
                "    `page` map<STRING,STRING>,\n" +
                "    `ts` bigint,\n" +
                "    row_time AS TO_TIMESTAMP_LTZ(ts,3),\n" +
                "    WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND\n" +
                " )" + SQLUtil.getKafkaDDLSource(TOPIC_DWD_TRAFFIC_PAGE, ckAndGroupId));


        Table keywordsTable = tableEnv.sqlQuery(" select\n" +
                "    page['item'] keywords,\n" +
                "    `ts`,\n" +
                "    `row_time`\n" +
                " from page_info\n" +
                " where page['last_page_id'] = 'search'\n" +
                " and page['item_type'] = 'keyword'\n" +
                " and page['item'] is not null"
        );

        tableEnv.createTemporaryView("keywordsTable", keywordsTable);

//        tableEnv.executeSql("select * from keywordsTable").print();

        tableEnv.createTemporarySystemFunction("KwSplit", KwSplit.class);

        Table keywordsSplitTable = tableEnv.sqlQuery("select keywords, keyword, row_time\n" +
                " FROM keywordsTable\n" +
                " LEFT JOIN LATERAL TABLE(KwSplit(keywords)) ON TRUE");

        tableEnv.createTemporaryView("keywordsSplitTable", keywordsSplitTable);

//        tableEnv.executeSql("select * from keywordsSplitTable").print();

        Table windowAggTable = tableEnv.sqlQuery("select\n" +
                "    cast(TUMBLE_START(row_time, INTERVAL '1' HOUR) as STRING) stt,\n" +
                "    cast(TUMBLE_END(row_time, INTERVAL '1' HOUR) as STRING) edt,\n" +
                "    cast(CURRENT_DATE as STRING) cur_date,\n" +
                "    keyword,\n" +
                "    count(*) keyword_count\n" +
                "from keywordsSplitTable\n" +
                "group by TUMBLE(row_time, INTERVAL '1' HOUR), keyword"
        );

        tableEnv.createTemporaryView("windowAggTable", windowAggTable);

//        tableEnv.executeSql("select * from windowAggTable").print();

        //写出到 doris
        //flink必须打开检查点才能将数据写出到 doris
        tableEnv.executeSql("create Table doris_sink(\n" +
                "    stt STRING,\n" +
                "    edt STRING,\n" +
                "    cur_date STRING,\n" +
                "    keyword STRING,\n" +
                "    keyword_count BIGINT\n" +
                ")" + SQLUtil.getDorisSinkSQL(DWS_TRAFFIC_SOURCE_KEYWORD_PAGE_VIEW_WINDOW));

        windowAggTable.insertInto("doris_sink").execute();

    }
}
















































