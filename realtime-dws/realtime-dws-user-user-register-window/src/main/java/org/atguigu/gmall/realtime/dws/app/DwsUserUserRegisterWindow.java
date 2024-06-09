package org.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.atguigu.gmall.realtime.common.base.BaseAPP;
import org.atguigu.gmall.realtime.common.bean.UserRegisterBean;
import org.atguigu.gmall.realtime.common.function.DorisMapFunction;
import org.atguigu.gmall.realtime.common.util.DateFormatUtil;
import org.atguigu.gmall.realtime.common.util.FlinkSinkUtil;

import java.time.Duration;

import static org.atguigu.gmall.realtime.common.constant.Constant.DWS_USER_USER_REGISTER_WINDOW;
import static org.atguigu.gmall.realtime.common.constant.Constant.TOPIC_DWD_USER_REGISTER;

public class DwsUserUserRegisterWindow extends BaseAPP {

    public static void main(String[] args) {
        new DwsUserUserRegisterWindow().start(10025, 1, "dws_user_user_register_window", TOPIC_DWD_USER_REGISTER);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        SingleOutputStreamOperator<UserRegisterBean> beanStream = stream.flatMap(new FlatMapFunction<String, UserRegisterBean>() {
            @Override
            public void flatMap(String s, Collector<UserRegisterBean> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    String createTime = jsonObject.getString("create_time");
                    String id = jsonObject.getString("id");
                    if (createTime != null && id != null) {
                        collector.collect(new UserRegisterBean("", "", "", 1L, createTime));
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("异常数据 >> " + s);
                }
            }
        });

        SingleOutputStreamOperator<UserRegisterBean> withWaterMarkStream = beanStream.assignTimestampsAndWatermarks(WatermarkStrategy.<UserRegisterBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L)).withTimestampAssigner(new SerializableTimestampAssigner<UserRegisterBean>() {
            @Override
            public long extractTimestamp(UserRegisterBean userRegisterBean, long l) {
                return DateFormatUtil.dateTimeToTs(userRegisterBean.getCreateTime());
            }
        }));

        SingleOutputStreamOperator<UserRegisterBean> reduceStream = withWaterMarkStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(5L))).reduce(new ReduceFunction<UserRegisterBean>() {
            @Override
            public UserRegisterBean reduce(UserRegisterBean userRegisterBean, UserRegisterBean t1) throws Exception {
                userRegisterBean.setRegisterCt(userRegisterBean.getRegisterCt() + t1.getRegisterCt());
                return userRegisterBean;
            }
        }, new ProcessAllWindowFunction<UserRegisterBean, UserRegisterBean, TimeWindow>() {
            @Override
            public void process(ProcessAllWindowFunction<UserRegisterBean, UserRegisterBean, TimeWindow>.Context context, Iterable<UserRegisterBean> iterable, Collector<UserRegisterBean> collector) throws Exception {
                TimeWindow window = context.window();
                long start = window.getStart();
                long end = window.getEnd();
                String stt = DateFormatUtil.tsToDateTime(start);
                String edt = DateFormatUtil.tsToDateTime(end);
                String curDt = DateFormatUtil.tsToDateForPartition(System.currentTimeMillis());
                for (UserRegisterBean userRegisterBean : iterable) {
                    userRegisterBean.setStt(stt);
                    userRegisterBean.setEdt(edt);
                    userRegisterBean.setCurDate(curDt);
                    collector.collect(userRegisterBean);
                }

            }
        });

        reduceStream.map(new DorisMapFunction<>()).sinkTo(FlinkSinkUtil.getDorisSink(DWS_USER_USER_REGISTER_WINDOW));
    }
}



















