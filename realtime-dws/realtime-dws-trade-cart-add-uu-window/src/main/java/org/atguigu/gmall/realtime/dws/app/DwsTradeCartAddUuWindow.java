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
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.atguigu.gmall.realtime.common.base.BaseAPP;
import org.atguigu.gmall.realtime.common.bean.CartAddUuBean;
import org.atguigu.gmall.realtime.common.function.DorisMapFunction;
import org.atguigu.gmall.realtime.common.util.DateFormatUtil;
import org.atguigu.gmall.realtime.common.util.FlinkSinkUtil;

import java.time.Duration;

import static org.atguigu.gmall.realtime.common.constant.Constant.DWS_TRADE_CART_ADD_UU_WINDOW;
import static org.atguigu.gmall.realtime.common.constant.Constant.TOPIC_DWD_TRADE_CART_ADD;

public class DwsTradeCartAddUuWindow extends BaseAPP {

    public static void main(String[] args) {
        new DwsTradeCartAddUuWindow().start(10026, 1, "dws_trade_cart_add_uu_window", TOPIC_DWD_TRADE_CART_ADD);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        SingleOutputStreamOperator<JSONObject> jsonObjStream = stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    String userId = jsonObject.getString("user_id");
                    Long ts = jsonObject.getLong("ts");
                    if (ts != null && userId != null) {
                        jsonObject.put("ts", ts * 1000L);
                        collector.collect(jsonObject);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("异常数据 >> " + s);
                }
            }
        });

        SingleOutputStreamOperator<JSONObject> withWaterMarkStream = jsonObjStream.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject jsonObject, long l) {
                return jsonObject.getLong("ts");
            }
        }));

        KeyedStream<JSONObject, String> keyedStream = withWaterMarkStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObject) throws Exception {
//                System.out.println(jsonObject.getString("user_id"));
                return jsonObject.getString("user_id");
            }
        });


        SingleOutputStreamOperator<CartAddUuBean> uuCtBeanStream = keyedStream.process(new KeyedProcessFunction<String, JSONObject, CartAddUuBean>() {

            ValueState lastLoginDtState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<JSONObject> lastLoginDt = new ValueStateDescriptor<>("last_login_dt", JSONObject.class);
                lastLoginDt.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                lastLoginDtState = getRuntimeContext().getState(lastLoginDt);
            }

            @Override
            public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, CartAddUuBean>.Context context, Collector<CartAddUuBean> collector) throws Exception {
                String curDt = DateFormatUtil.tsToDate(jsonObject.getLong("ts"));
                Object lastLoginDt = lastLoginDtState.value();
                if (lastLoginDt == null || !lastLoginDt.equals(curDt)) {
                    //是独立用户
                    lastLoginDtState.update(curDt);
                    collector.collect(new CartAddUuBean("", "", "", 1L));
                }
            }
        });

        SingleOutputStreamOperator<CartAddUuBean> reduceStream = uuCtBeanStream.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L))).reduce(new ReduceFunction<CartAddUuBean>() {
            @Override
            public CartAddUuBean reduce(CartAddUuBean cartAddUuBean, CartAddUuBean t1) throws Exception {
                cartAddUuBean.setCartAddUuCt(cartAddUuBean.getCartAddUuCt() + t1.getCartAddUuCt());
                return cartAddUuBean;
            }
        }, new ProcessAllWindowFunction<CartAddUuBean, CartAddUuBean, TimeWindow>() {
            @Override
            public void process(ProcessAllWindowFunction<CartAddUuBean, CartAddUuBean, TimeWindow>.Context context, Iterable<CartAddUuBean> iterable, Collector<CartAddUuBean> collector) throws Exception {
                TimeWindow window = context.window();
                String stt = DateFormatUtil.tsToDateTime(window.getStart());
                String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                String ts = DateFormatUtil.tsToDateForPartition(System.currentTimeMillis());
                for (CartAddUuBean cartAddUuBean : iterable) {
                    cartAddUuBean.setStt(stt);
                    cartAddUuBean.setEdt(edt);
                    cartAddUuBean.setCurDate(ts);
                    collector.collect(cartAddUuBean);
                }
            }
        });

        reduceStream.map(new DorisMapFunction<>()).sinkTo(FlinkSinkUtil.getDorisSink(DWS_TRADE_CART_ADD_UU_WINDOW));
    }
}











