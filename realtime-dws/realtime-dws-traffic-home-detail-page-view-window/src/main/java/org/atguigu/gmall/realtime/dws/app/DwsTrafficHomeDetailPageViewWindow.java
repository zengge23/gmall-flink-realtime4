package org.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.atguigu.gmall.realtime.common.base.BaseAPP;
import org.atguigu.gmall.realtime.common.function.DorisMapFunction;
import org.atguigu.gmall.realtime.common.util.DateFormatUtil;
import org.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import org.atguigu.gmall.realtime.dws.bean.TrafficHomeDetailPageViewBean;

import java.time.Duration;

import static org.atguigu.gmall.realtime.common.constant.Constant.DWS_TRAFFIC_HOME_DETAIL_PAGE_VIEW_WINDOW;
import static org.atguigu.gmall.realtime.common.constant.Constant.TOPIC_DWD_TRAFFIC_PAGE;

public class DwsTrafficHomeDetailPageViewWindow extends BaseAPP {

    public static void main(String[] args) {
        new DwsTrafficHomeDetailPageViewWindow().start(10023, 4, "dws_traffic_home_detail_page_view_window", TOPIC_DWD_TRAFFIC_PAGE);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        SingleOutputStreamOperator<JSONObject> jsonObjStream = stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    JSONObject page = jsonObject.getJSONObject("page");
                    String pageId = page.getString("page_id");
                    String mid = jsonObject.getJSONObject("common").getString("mid");
                    if ("home".equals(pageId) || "good_detail".equals(pageId)) {
                        if (mid != null) {
                            collector.collect(jsonObject);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("异常数据 >> " + s);
                }
            }
        });

        KeyedStream<JSONObject, String> keyedStream = jsonObjStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObject) throws Exception {
                return jsonObject.getJSONObject("common").getString("mid");
            }
        });

        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> processBeanStream = keyedStream.process(new KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageViewBean>() {
            ValueState<String> homeLastLoginState;
            ValueState<String> detailLastLoginState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("home_last_login", String.class);
                stateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1L)).build());
                homeLastLoginState = getRuntimeContext().getState(stateDescriptor);

                ValueStateDescriptor<String> detailStateDescriptor = new ValueStateDescriptor<>("detail_last_login", String.class);
                detailStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1L)).build());
                detailLastLoginState = getRuntimeContext().getState(detailStateDescriptor);
            }

            @Override
            public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageViewBean>.Context context, Collector<TrafficHomeDetailPageViewBean> collector) throws Exception {
                String pageId = jsonObject.getJSONObject("page").getString("page_id");
                Long ts = jsonObject.getLong("ts");
                String curDt = DateFormatUtil.tsToDate(ts);
                Long homeUvCt = 0L;
                Long goodDetailUvCt = 0L;
                if ("home".equals(pageId)) {
                    String homeLastLoginDt = homeLastLoginState.value();
                    if (homeLastLoginDt == null || !homeLastLoginDt.equals(curDt)) {
                        homeUvCt = 1l;
                        homeLastLoginState.update(curDt);
                    }
                } else {
                    String detailLastLoginDt = detailLastLoginState.value();
                    if (detailLastLoginDt == null || !detailLastLoginDt.equals(curDt)) {
                        goodDetailUvCt = 1L;
                        detailLastLoginState.update(curDt);
                    }
                }

                if (homeUvCt != 0 || goodDetailUvCt != 0) {
                    collector.collect(TrafficHomeDetailPageViewBean.builder()
                            .homeUvCt(homeUvCt)
                            .goodDetailUvCt(goodDetailUvCt)
                            .ts(ts)
                            .build());
                }
            }
        });

        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> withWaterMarkStream = processBeanStream.assignTimestampsAndWatermarks(WatermarkStrategy.<TrafficHomeDetailPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L)).withTimestampAssigner(new SerializableTimestampAssigner<TrafficHomeDetailPageViewBean>() {
            @Override
            public long extractTimestamp(TrafficHomeDetailPageViewBean trafficHomeDetailPageViewBean, long l) {
                return trafficHomeDetailPageViewBean.getTs();
            }
        }));

        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> reduceStream = withWaterMarkStream.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)))
                .reduce(new ReduceFunction<TrafficHomeDetailPageViewBean>() {
                    @Override
                    public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean trafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean t1) throws Exception {
                        trafficHomeDetailPageViewBean.setHomeUvCt(trafficHomeDetailPageViewBean.getHomeUvCt() + t1.getHomeUvCt());
                        trafficHomeDetailPageViewBean.setGoodDetailUvCt(trafficHomeDetailPageViewBean.getGoodDetailUvCt() + t1.getGoodDetailUvCt());
                        return trafficHomeDetailPageViewBean;
                    }
                }, new AllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<TrafficHomeDetailPageViewBean> iterable, Collector<TrafficHomeDetailPageViewBean> collector) throws Exception {
                        String stt = DateFormatUtil.tsToDateTime(timeWindow.getStart());
                        String edt = DateFormatUtil.tsToDateTime(timeWindow.getEnd());
                        String curDt = DateFormatUtil.tsToDateForPartition(System.currentTimeMillis());
                        for (TrafficHomeDetailPageViewBean trafficHomeDetailPageViewBean : iterable) {
                            trafficHomeDetailPageViewBean.setStt(stt);
                            trafficHomeDetailPageViewBean.setEdt(edt);
                            trafficHomeDetailPageViewBean.setCurDate(curDt);
                            collector.collect(trafficHomeDetailPageViewBean);
                        }
                    }
                });

        reduceStream.map(new DorisMapFunction<>()).sinkTo(FlinkSinkUtil.getDorisSink(DWS_TRAFFIC_HOME_DETAIL_PAGE_VIEW_WINDOW));
    }
}
































