package org.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.atguigu.gmall.realtime.common.base.BaseAPP;
import org.atguigu.gmall.realtime.common.bean.UserLoginBean;
import org.atguigu.gmall.realtime.common.function.DorisMapFunction;
import org.atguigu.gmall.realtime.common.util.DateFormatUtil;
import org.atguigu.gmall.realtime.common.util.FlinkSinkUtil;

import java.time.Duration;

import static org.atguigu.gmall.realtime.common.constant.Constant.DWS_USER_USER_LOGIN_WINDOW;
import static org.atguigu.gmall.realtime.common.constant.Constant.TOPIC_DWD_TRAFFIC_PAGE;

public class DwsUserUserLoginWindow extends BaseAPP {

    public static void main(String[] args) {
        new DwsUserUserLoginWindow().start(10024, 1, "dws_user_user_login_window", TOPIC_DWD_TRAFFIC_PAGE);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        SingleOutputStreamOperator<JSONObject> jsonObjStream = stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject obj = JSONObject.parseObject(s);
                    String uid = obj.getJSONObject("common").getString("uid");
                    String lastPageId = obj.getJSONObject("page").getString("last_page_id");
                    Long ts = obj.getLong("ts");
                    if (uid != null && (lastPageId != null || "login".equals(lastPageId)) && ts != null) {
                        collector.collect(obj);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("异常数据 >> " + s);
                }
            }
        });

        SingleOutputStreamOperator<JSONObject> withWaterMarkStream = jsonObjStream.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5L)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject jsonObject, long l) {
                return jsonObject.getLong("ts");
            }
        }));

        KeyedStream<JSONObject, String> keyedStream = withWaterMarkStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObject) throws Exception {
                return jsonObject.getJSONObject("common").getString("uid");
            }
        });

        SingleOutputStreamOperator<UserLoginBean> uuCtBeanStream = keyedStream.process(new KeyedProcessFunction<String, JSONObject, UserLoginBean>() {

            ValueState<String> lastLoginDtState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("last_login_dt", String.class);
                lastLoginDtState = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, UserLoginBean>.Context context, Collector<UserLoginBean> collector) throws Exception {
                String lastLoginDt = lastLoginDtState.value();
                Long ts = jsonObject.getLong("ts");
                String curDt = DateFormatUtil.tsToDate(ts);
                //回流用户数
                Long backCt = 0L;
                //独立用户数
                Long uuCt = 0L;
                //判断独立用户
                if (lastLoginDt == null || !lastLoginDt.equals(curDt)) {
                    uuCt = 1L;
                }
                //判断回流用户
                if (lastLoginDt != null && ts - DateFormatUtil.dateToTs(lastLoginDt) > 7 * 24 * 60 * 60 * 1000L) {
                    //是回流用户
                    backCt = 1L;
                }
                lastLoginDtState.update(curDt);
                //不是独立用户肯定不是回流用户 不需要下游统计
                if (uuCt != 0) {
                    collector.collect(new UserLoginBean("", "", "", backCt, uuCt, ts));
                }
            }
        });
        SingleOutputStreamOperator<UserLoginBean> reduceStream = uuCtBeanStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(10L))).reduce(new ReduceFunction<UserLoginBean>() {
            @Override
            public UserLoginBean reduce(UserLoginBean userLoginBean, UserLoginBean t1) throws Exception {
                userLoginBean.setBackCt(userLoginBean.getBackCt() + t1.getBackCt());
                userLoginBean.setUuCt(userLoginBean.getUuCt() + t1.getUuCt());
                return userLoginBean;
            }
        }, new ProcessAllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>() {
            @Override
            public void process(ProcessAllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>.Context context, Iterable<UserLoginBean> iterable, Collector<UserLoginBean> collector) throws Exception {
                TimeWindow window = context.window();
                String stt = DateFormatUtil.tsToDateTime(window.getStart());
                String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                String curDt = DateFormatUtil.tsToDateForPartition(System.currentTimeMillis());
                for (UserLoginBean userLoginBean : iterable) {
                    userLoginBean.setStt(stt);
                    userLoginBean.setEdt(edt);
                    userLoginBean.setCurDate(curDt);
                    collector.collect(userLoginBean);
                }
            }
        });

        reduceStream.map(new DorisMapFunction<>()).sinkTo(FlinkSinkUtil.getDorisSink(DWS_USER_USER_LOGIN_WINDOW));

    }
}


















