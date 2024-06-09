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
import org.atguigu.gmall.realtime.common.bean.TradeOrderBean;
import org.atguigu.gmall.realtime.common.function.DorisMapFunction;
import org.atguigu.gmall.realtime.common.util.DateFormatUtil;
import org.atguigu.gmall.realtime.common.util.FlinkSinkUtil;

import java.time.Duration;

import static org.atguigu.gmall.realtime.common.constant.Constant.DWS_TRADE_ORDER_WINDOW;
import static org.atguigu.gmall.realtime.common.constant.Constant.TOPIC_DWD_TRADE_ORDER_DETAIL;

public class DwsTradeOrderWindow extends BaseAPP {

    public static void main(String[] args) {
        new DwsTradeOrderWindow().start(10028, 1, "dws_trade_order_window", TOPIC_DWD_TRADE_ORDER_DETAIL);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        SingleOutputStreamOperator<JSONObject> mapStream = stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    String userId = jsonObject.getString("user_id");
                    String orderId = jsonObject.getString("order_id");
                    Long ts = jsonObject.getLong("ts") * 1000;
                    jsonObject.put("ts", ts);
                    if (userId != null && orderId != null) {
                        collector.collect(jsonObject);
                    }
                } catch (Exception e) {
                    System.err.println("异常数据 >> " + s);
                }
            }
        });

        SingleOutputStreamOperator<JSONObject> withWaterMarkStream = mapStream.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5L)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject jsonObject, long l) {
//                System.out.println(">>" + jsonObject.toJSONString());
                return jsonObject.getLong("ts");
            }
        }).withIdleness(Duration.ofSeconds(60L)));

        KeyedStream<JSONObject, String> keyedStream = withWaterMarkStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObject) throws Exception {
//                System.out.println("??? " + jsonObject.toJSONString());
                return jsonObject.getString("user_id");
            }
        });

        SingleOutputStreamOperator<TradeOrderBean> processStream = keyedStream.process(new KeyedProcessFunction<String, JSONObject, TradeOrderBean>() {
            ValueState<String> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("last_pay_date_state", String.class);
                valueState = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, TradeOrderBean>.Context context, Collector<TradeOrderBean> collector) throws Exception {
//                System.out.println(jsonObject);
                String lastPayDate = valueState.value();
                long payUuCount = 0L;
                long payNewCount = 0L;
                Long ts = jsonObject.getLong("ts");
                String curDt = DateFormatUtil.tsToDateTime(ts);
                if (lastPayDate == null) {
                    payUuCount = 1L;
                    payNewCount = 1L;
                    valueState.update(curDt);
                } else {
                    if (!lastPayDate.equals(curDt)) {
                        payUuCount = 1L;
                        valueState.update(curDt);
                    }
                }
                if (payUuCount == 1) {
                    collector.collect(TradeOrderBean.builder()
                            .orderNewUserCount(payNewCount)
                            .orderUniqueUserCount(payUuCount)
                            .ts(ts)
                            .build());
                }
            }
        });

        SingleOutputStreamOperator<TradeOrderBean> reduceStream = processStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(5L))).reduce(new ReduceFunction<TradeOrderBean>() {
            @Override
            public TradeOrderBean reduce(TradeOrderBean tradeOrderBean, TradeOrderBean t1) throws Exception {
                tradeOrderBean.setOrderNewUserCount(tradeOrderBean.getOrderNewUserCount() + t1.getOrderNewUserCount());
                tradeOrderBean.setOrderUniqueUserCount(tradeOrderBean.getOrderUniqueUserCount() + t1.getOrderUniqueUserCount());
                return tradeOrderBean;
            }
        }, new ProcessAllWindowFunction<TradeOrderBean, TradeOrderBean, TimeWindow>() {
            @Override
            public void process(ProcessAllWindowFunction<TradeOrderBean, TradeOrderBean, TimeWindow>.Context context, Iterable<TradeOrderBean> iterable, Collector<TradeOrderBean> collector) throws Exception {
                TimeWindow window = context.window();
                String stt = DateFormatUtil.tsToDateTime(window.getStart());
                String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                String curDt = DateFormatUtil.tsToDateForPartition(System.currentTimeMillis());
                TradeOrderBean next = iterable.iterator().next();
                next.setStt(stt);
                next.setEdt(edt);
                next.setCurDate(curDt);

                collector.collect(next);
            }
        });
        reduceStream.map(new DorisMapFunction<>()).sinkTo(FlinkSinkUtil.getDorisSink(DWS_TRADE_ORDER_WINDOW));
    }
}
































