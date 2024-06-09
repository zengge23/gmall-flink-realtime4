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
import org.atguigu.gmall.realtime.common.bean.TradePaymentBean;
import org.atguigu.gmall.realtime.common.function.DorisMapFunction;
import org.atguigu.gmall.realtime.common.util.DateFormatUtil;
import org.atguigu.gmall.realtime.common.util.FlinkSinkUtil;

import java.time.Duration;

import static org.atguigu.gmall.realtime.common.constant.Constant.DWS_TRADE_PAYMENT_SUC_WINDOW;
import static org.atguigu.gmall.realtime.common.constant.Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS;

public class DwsTradePaymentSucWindow extends BaseAPP {

    public static void main(String[] args) {
        new DwsTradePaymentSucWindow().start(10027, 1, "dws_trade_payment_suc_window", TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        SingleOutputStreamOperator<JSONObject> etlStream = stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    String userId = jsonObject.getString("user_id");
                    String orderId = jsonObject.getString("order_id");

                    Long ts = jsonObject.getLong("ts");
                    if (userId != null && orderId != null && ts != null) {
                        jsonObject.put("ts", ts * 1000);
                        collector.collect(jsonObject);
                    }
                } catch (Exception e) {
                    System.err.println("异常数据 >> " + s);
                }
            }
        });

        KeyedStream<JSONObject, String> keyedStream = etlStream.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5L)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject jsonObject, long l) {
                return jsonObject.getLong("ts");
            }
        }).withIdleness(Duration.ofSeconds(60L))).keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObject) throws Exception {
                return jsonObject.getString("user_id");
            }
        });

        SingleOutputStreamOperator<TradePaymentBean> processStream = keyedStream.process(new KeyedProcessFunction<String, JSONObject, TradePaymentBean>() {
            ValueState<String> lastPayDateState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("last_pay_date_state", String.class);
                lastPayDateState = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, TradePaymentBean>.Context context, Collector<TradePaymentBean> collector) throws Exception {
                String lastPayDate = lastPayDateState.value();
                Long ts = jsonObject.getLong("ts");
                String curDt = DateFormatUtil.tsToDate(ts);
                long payUuCount = 0L;
                long payNewCount = 0L;
                //今日第一次支付成功
                if (!curDt.equals(lastPayDate)) {
                    lastPayDateState.update(curDt);
                    payUuCount = 1L;
                    //没有上次支付时间,是新用户支付
                    if (lastPayDate == null) {
                        payNewCount = 1L;
                    }
                }
                if (payUuCount == 1L) {
                    collector.collect(new TradePaymentBean("", "", "", payUuCount, payNewCount, ts));
                }
            }
        });

        SingleOutputStreamOperator<TradePaymentBean> reduceStream = processStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(5L))).reduce(new ReduceFunction<TradePaymentBean>() {
            @Override
            public TradePaymentBean reduce(TradePaymentBean tradePaymentBean, TradePaymentBean t1) throws Exception {
                tradePaymentBean.setPaymentSucNewUserCount(tradePaymentBean.getPaymentSucNewUserCount() + t1.getPaymentSucNewUserCount());
                tradePaymentBean.setPaymentSucUniqueUserCount(tradePaymentBean.getPaymentSucUniqueUserCount() + t1.getPaymentSucUniqueUserCount());
                return tradePaymentBean;
            }
        }, new ProcessAllWindowFunction<TradePaymentBean, TradePaymentBean, TimeWindow>() {
            @Override
            public void process(ProcessAllWindowFunction<TradePaymentBean, TradePaymentBean, TimeWindow>.Context context, Iterable<TradePaymentBean> iterable, Collector<TradePaymentBean> collector) throws Exception {
                TradePaymentBean next = iterable.iterator().next();
                next.setStt(DateFormatUtil.tsToDateTime(context.window().getStart()));
                next.setEdt(DateFormatUtil.tsToDateTime(context.window().getEnd()));
                next.setCurDate(DateFormatUtil.tsToDateForPartition(System.currentTimeMillis()));
                collector.collect(next);
            }
        });

        reduceStream.map(new DorisMapFunction<>()).sinkTo(FlinkSinkUtil.getDorisSink(DWS_TRADE_PAYMENT_SUC_WINDOW));
    }
}
