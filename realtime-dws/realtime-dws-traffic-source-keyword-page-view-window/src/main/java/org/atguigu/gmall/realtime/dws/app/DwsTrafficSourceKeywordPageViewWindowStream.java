package org.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.atguigu.gmall.realtime.common.base.BaseAPP;
import org.atguigu.gmall.realtime.common.bean.KeyWordCountBean;
import org.atguigu.gmall.realtime.common.util.DateFormatUtil;
import org.atguigu.gmall.realtime.dws.util.IkUtil;

import java.time.Duration;
import java.util.*;

import static org.atguigu.gmall.realtime.common.constant.Constant.TOPIC_DWD_TRAFFIC_PAGE;

public class DwsTrafficSourceKeywordPageViewWindowStream extends BaseAPP {
    public static void main(String[] args) {
        new DwsTrafficSourceKeywordPageViewWindowStream().start(9999, 1, "dws_traffic_source_keyword_page_view_window", TOPIC_DWD_TRAFFIC_PAGE);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
//        stream.print();
        SingleOutputStreamOperator<JSONObject> mapStream = stream.flatMap(
                new FlatMapFunction<String, JSONObject>() {
                    @Override
                    public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                        try {
                            JSONObject jsonObject = JSONObject.parseObject(s);
                            JSONObject page = jsonObject.getJSONObject("page");
                            String lastPageId = page.getString("last_page_id");
                            String type = page.getString("item_type");
                            String item = page.getString("item");
                            if (lastPageId != null || type != null || item != null) {
                                if ("search".equals(lastPageId) && "keyword".equals(type)) {
//                                    System.out.println(jsonObject);
                                    collector.collect(jsonObject);
                                }
                            }
                        } catch (Exception e) {
                            System.out.println("异常数据 >> " + s);
                        }
                    }
                }
        );

        SingleOutputStreamOperator<KeyWordCountBean> processStream = mapStream.process(new ProcessFunction<JSONObject, KeyWordCountBean>() {
            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, KeyWordCountBean>.Context context, Collector<KeyWordCountBean> collector) throws Exception {
                JSONObject page = jsonObject.getJSONObject("page");
                Long ts = jsonObject.getLong("ts");
                String item = page.getString("item");
                Set<String> split = IkUtil.split(item);
                for (String str : split) {
                    collector.collect(KeyWordCountBean.builder()
                            .keywords(item)
                            .keyword(str)
                            .ts(ts)
                            .keywordCount(1L)
                            .build());
                }
            }
        });

//        processStream.print();

        SingleOutputStreamOperator<KeyWordCountBean> withWaterMarkStream = processStream.assignTimestampsAndWatermarks(WatermarkStrategy.<KeyWordCountBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L)).withTimestampAssigner(new SerializableTimestampAssigner<KeyWordCountBean>() {
            @Override
            public long extractTimestamp(KeyWordCountBean jsonObject, long l) {
                return jsonObject.getTs();
            }
        }));

//        withWaterMarkStream.print();

        SingleOutputStreamOperator<KeyWordCountBean> reduceStream = withWaterMarkStream.windowAll(TumblingEventTimeWindows.of(Time.minutes(480L)))
                .apply(new AllWindowFunction<KeyWordCountBean, KeyWordCountBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<KeyWordCountBean> iterable, Collector<KeyWordCountBean> collector) throws Exception {
                        Map<String, KeyWordCountBean> map = new HashMap<String, KeyWordCountBean>();
                        long start = timeWindow.getStart();
                        long end = timeWindow.getEnd();
                        String stt = DateFormatUtil.tsToDateTime(start);
                        String edt = DateFormatUtil.tsToDateTime(end);
                        String curDt = DateFormatUtil.tsToDateForPartition(System.currentTimeMillis());
                        for (KeyWordCountBean keyWordCountBean : iterable) {
                            String keyword1 = keyWordCountBean.getKeyword();
                            KeyWordCountBean keyWordCountBean1 = map.get(keyword1);
                            if (keyWordCountBean1 == null) {
                                map.put(keyword1, keyWordCountBean);
                            } else {
                                if (Objects.equals(keyWordCountBean1.getKeyword(), keyWordCountBean.getKeyword())) {
                                    keyWordCountBean.setKeywordCount(keyWordCountBean1.getKeywordCount() + keyWordCountBean.getKeywordCount());
                                }
                            }
                            keyWordCountBean.setStt(stt);
                            keyWordCountBean.setEdt(edt);
                            keyWordCountBean.setCurDate(curDt);
                            map.put(keyword1, keyWordCountBean);
                        }
                        Collection<KeyWordCountBean> keyWordCountBeans = map.values();
                        for (KeyWordCountBean keyWordCountBean : keyWordCountBeans) {
                            collector.collect(keyWordCountBean);
                        }
                    }
                });
//                .reduce(new ReduceFunction<KeyWordCountBean>() {
//                    Map<String, KeyWordCountBean> map = new HashMap<String, KeyWordCountBean>();
//
//                    @Override
//                    public KeyWordCountBean reduce(KeyWordCountBean value1, KeyWordCountBean value2) throws Exception {
//                        String keyword1 = value1.getKeyword();
//                        String keyword2 = value2.getKeyword();
//                        if (Objects.equals(keyword1, keyword2)) {
//                            value1.setKeywordCount(value1.getKeywordCount() + value2.getKeywordCount());
//                            map.put(keyword1, value1);
//                        }else {
//                            map.put(keyword1, value1);
//                        }
//                        return value1;
//                    }
//                },
//                        new AllWindowFunction<KeyWordCountBean, KeyWordCountBean, TimeWindow>() {
//                    @Override
//                    public void apply(TimeWindow timeWindow, Iterable<KeyWordCountBean> iterable, Collector<KeyWordCountBean> collector) throws Exception {
//                        long start = timeWindow.getStart();
//                        long end = timeWindow.getEnd();
//                        String stt = DateFormatUtil.tsToDateTime(start);
//                        String edt = DateFormatUtil.tsToDateTime(end);
//                        String curDt = DateFormatUtil.tsToDateForPartition(System.currentTimeMillis());
//                        for (KeyWordCountBean keyWordCountBean : iterable) {
//                            keyWordCountBean.setStt(stt);
//                            keyWordCountBean.setEdt(edt);
//                            keyWordCountBean.setCurDate(curDt);
//                            collector.collect(keyWordCountBean);
//                        }
//                    }
//                });

        reduceStream.print();
    }
}
