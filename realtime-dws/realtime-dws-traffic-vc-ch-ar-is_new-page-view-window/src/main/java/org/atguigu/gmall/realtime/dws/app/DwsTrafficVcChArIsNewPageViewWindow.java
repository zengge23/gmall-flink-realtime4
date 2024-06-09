package org.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.atguigu.gmall.realtime.common.base.BaseAPP;
import org.atguigu.gmall.realtime.common.bean.TrafficPageViewBean;
import org.atguigu.gmall.realtime.common.util.DateFormatUtil;
import org.atguigu.gmall.realtime.common.util.FlinkSinkUtil;

import java.time.Duration;

import static org.atguigu.gmall.realtime.common.constant.Constant.DWS_TRAFFIC_VC_CH_AR_IS_NEW_PAGE_VIEW_WINDOW;
import static org.atguigu.gmall.realtime.common.constant.Constant.TOPIC_DWD_TRAFFIC_PAGE;

public class DwsTrafficVcChArIsNewPageViewWindow extends BaseAPP {

    public static void main(String[] args) {
        new DwsTrafficVcChArIsNewPageViewWindow().start(10022, 4, "dws_traffic_vc_ch_ar_is_new_page_view_window", TOPIC_DWD_TRAFFIC_PAGE);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        SingleOutputStreamOperator<JSONObject> jsonObjStream = stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    Long ts = jsonObject.getLong("ts");
                    String mid = jsonObject.getJSONObject("common").getString("mid");
                    if (mid != null && ts != null) {
                        collector.collect(jsonObject);
                    }
                } catch (Exception e) {
                    System.out.println("过滤脏数据" + s);
                }
            }
        });

        SingleOutputStreamOperator<TrafficPageViewBean> beanStream = jsonObjStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObject) throws Exception {
                return jsonObject.getJSONObject("common").getString("mid");
            }
        }).process(new KeyedProcessFunction<String, JSONObject, TrafficPageViewBean>() {
            private ValueState<String> lastLoginDtState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("last_login_dt", String.class);
                stateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(org.apache.flink.api.common.time.Time.days(1L)).setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite).build());
                lastLoginDtState = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, TrafficPageViewBean>.Context context, Collector<TrafficPageViewBean> collector) throws Exception {
                Long ts = jsonObject.getLong("ts");
                JSONObject common = jsonObject.getJSONObject("common");
                JSONObject page = jsonObject.getJSONObject("page");
                String curDt = DateFormatUtil.tsToDate(ts);
                String lastLoginDt = lastLoginDtState.value();
                Long uvCt = 0L;
                Long svCt = 0L;
                if (lastLoginDt == null || !lastLoginDt.equals(curDt)) {
                    uvCt = 1L;
                    lastLoginDtState.update(curDt);
                }
                String lastPageId = page.getString("last_page_id");
                if (lastPageId == null) {
                    svCt = 1L;
                }
                collector.collect(TrafficPageViewBean.builder()
                        .vc(common.getString("vc"))
                        .ar(common.getString("ar"))
                        .ch(common.getString("ch"))
                        .isNew(common.getString("is_new"))
                        .uvCt(uvCt)
                        .svCt(svCt)
                        .pvCt(1L)
                        .durSum(page.getLong("during_time"))
                        .sid(common.getString("sid")).ts(ts)
                        .build());
            }
        });

        SingleOutputStreamOperator<TrafficPageViewBean> withWaterMarkStream = beanStream.assignTimestampsAndWatermarks(WatermarkStrategy.<TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(3L)).withTimestampAssigner(new SerializableTimestampAssigner<TrafficPageViewBean>() {
            @Override
            public long extractTimestamp(TrafficPageViewBean trafficPageViewBean, long l) {
                return trafficPageViewBean.getTs();
            }
        }));

        KeyedStream<TrafficPageViewBean, String> keyedStream = withWaterMarkStream.keyBy(new KeySelector<TrafficPageViewBean, String>() {
            @Override
            public String getKey(TrafficPageViewBean trafficPageViewBean) throws Exception {
                return trafficPageViewBean.getVc() + ":" + trafficPageViewBean.getCh() + ":" + trafficPageViewBean.getAr() + ":" + trafficPageViewBean.getIsNew();
            }
        });

        WindowedStream<TrafficPageViewBean, String, TimeWindow> windowedStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10L)));

        SingleOutputStreamOperator<TrafficPageViewBean> reduceFullStream = windowedStream.reduce(new ReduceFunction<TrafficPageViewBean>() {
            @Override
            public TrafficPageViewBean reduce(TrafficPageViewBean trafficPageViewBean, TrafficPageViewBean t1) throws Exception {
                trafficPageViewBean.setSvCt(trafficPageViewBean.getSvCt() + t1.getSvCt());
                trafficPageViewBean.setUvCt(trafficPageViewBean.getUvCt() + t1.getUvCt());
                trafficPageViewBean.setPvCt(trafficPageViewBean.getPvCt() + t1.getPvCt());
                trafficPageViewBean.setDurSum(trafficPageViewBean.getDurSum() + t1.getDurSum());
                return trafficPageViewBean;
            }

        }, new ProcessWindowFunction<TrafficPageViewBean, TrafficPageViewBean, String, TimeWindow>() {
            @Override
            public void process(String s, ProcessWindowFunction<TrafficPageViewBean, TrafficPageViewBean, String, TimeWindow>.Context context, Iterable<TrafficPageViewBean> iterable, Collector<TrafficPageViewBean> collector) throws Exception {
                TimeWindow window = context.window();
                String stt = DateFormatUtil.tsToDate(window.getStart());
                String edt = DateFormatUtil.tsToDate(window.getEnd());
                String curDt = DateFormatUtil.tsToDateForPartition(System.currentTimeMillis());
                iterable.forEach(trafficPageViewBean -> {
                            trafficPageViewBean.setStt(stt);
                            trafficPageViewBean.setEdt(edt);
                            trafficPageViewBean.setCur_date(curDt);
                            collector.collect(trafficPageViewBean);
                        }
                );
            }
        });

        reduceFullStream.map(new MapFunction<TrafficPageViewBean, String>() {
            @Override
            public String map(TrafficPageViewBean trafficPageViewBean) throws Exception {
                SerializeConfig serializeConfig = new SerializeConfig();
                serializeConfig.setPropertyNamingStrategy(PropertyNamingStrategy.SnakeCase);
                String jsonString = JSONObject.toJSONString(trafficPageViewBean, serializeConfig);
                System.out.println(jsonString);
                return jsonString;
            }
        }).sinkTo(FlinkSinkUtil.getDorisSink(DWS_TRAFFIC_VC_CH_AR_IS_NEW_PAGE_VIEW_WINDOW));
    }
}






























