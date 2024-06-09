package org.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.atguigu.gmall.realtime.common.base.BaseAPP;
import org.atguigu.gmall.realtime.common.bean.TradeProvinceOrderBean;
import org.atguigu.gmall.realtime.common.function.DimAsyncFunction;
import org.atguigu.gmall.realtime.common.function.DorisMapFunction;
import org.atguigu.gmall.realtime.common.util.DateFormatUtil;
import org.atguigu.gmall.realtime.common.util.FlinkSinkUtil;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

import static org.atguigu.gmall.realtime.common.constant.Constant.DWS_TRADE_PROVINCE_ORDER_WINDOW;
import static org.atguigu.gmall.realtime.common.constant.Constant.TOPIC_DWD_TRADE_ORDER_DETAIL;

public class DwsTradeProvinceOrderWindow extends BaseAPP {

    public static void main(String[] args) {
        new DwsTradeProvinceOrderWindow().start(10030, 1, "dws_trade_province_order_window", TOPIC_DWD_TRADE_ORDER_DETAIL);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        SingleOutputStreamOperator<JSONObject> jsonObjStream = stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                try {
                    if (s != null) {
                        JSONObject jsonObject = JSONObject.parseObject(s);
                        String id = jsonObject.getString("id");
                        String orderId = jsonObject.getString("order_id");
                        String provinceId = jsonObject.getString("province_id");
                        Long ts = jsonObject.getLong("ts");
                        if (id != null && orderId != null && provinceId != null && ts != null) {
                            jsonObject.put("ts", ts * 1000);
                            collector.collect(jsonObject);
                        }
                    }
                } catch (Exception e) {
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
                return jsonObject.getString("id");
            }
        });

        SingleOutputStreamOperator<TradeProvinceOrderBean> beanStream = keyedStream.map(new RichMapFunction<JSONObject, TradeProvinceOrderBean>() {
            ValueState<BigDecimal> lastTotalAmountState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<BigDecimal> lastTotalAmountDesc = new ValueStateDescriptor<>("last_total_amount", BigDecimal.class);
                lastTotalAmountDesc.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(30L)).build());
                lastTotalAmountState = getRuntimeContext().getState(lastTotalAmountDesc);
            }

            @Override
            public TradeProvinceOrderBean map(JSONObject jsonObject) throws Exception {
                HashSet<String> set = new HashSet<>();
                set.add(jsonObject.getString("order_id"));
                BigDecimal lastTotalAmount = lastTotalAmountState.value();
                lastTotalAmount = lastTotalAmount == null ? new BigDecimal("0") : lastTotalAmount;
                BigDecimal splitTotalAmount = jsonObject.getBigDecimal("split_total_amount");
                lastTotalAmountState.update(splitTotalAmount);
                return TradeProvinceOrderBean.builder()
                        .orderIdSet(set)
                        .provinceId(jsonObject.getString("province_id"))
                        .orderDetailId(jsonObject.getString("id"))
                        .ts(jsonObject.getLong("ts"))
                        .orderAmount(splitTotalAmount.subtract(lastTotalAmount))
                        .build();
            }
        });

        SingleOutputStreamOperator<TradeProvinceOrderBean> reduceStream = beanStream.keyBy(new KeySelector<TradeProvinceOrderBean, String>() {
                    @Override
                    public String getKey(TradeProvinceOrderBean tradeProvinceOrderBean) throws Exception {
                        return tradeProvinceOrderBean.getProvinceId();
                    }
                }).window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)))
                .reduce(new ReduceFunction<TradeProvinceOrderBean>() {
                    @Override
                    public TradeProvinceOrderBean reduce(TradeProvinceOrderBean tradeProvinceOrderBean, TradeProvinceOrderBean t1) throws Exception {
                        tradeProvinceOrderBean.setOrderAmount(tradeProvinceOrderBean.getOrderAmount().add(t1.getOrderAmount()));
                        tradeProvinceOrderBean.getOrderIdSet().addAll(t1.getOrderIdSet());
                        return tradeProvinceOrderBean;
                    }
                }, new ProcessWindowFunction<TradeProvinceOrderBean, TradeProvinceOrderBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<TradeProvinceOrderBean, TradeProvinceOrderBean, String, TimeWindow>.Context context, Iterable<TradeProvinceOrderBean> iterable, Collector<TradeProvinceOrderBean> collector) throws Exception {
                        TimeWindow window = context.window();
                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        String curDt = DateFormatUtil.tsToDateForPartition(System.currentTimeMillis());
                        for (TradeProvinceOrderBean tradeProvinceOrderBean : iterable) {
                            tradeProvinceOrderBean.setStt(stt);
                            tradeProvinceOrderBean.setEdt(edt);
                            tradeProvinceOrderBean.setCurDate(curDt);
                            tradeProvinceOrderBean.setOrderCount((long) tradeProvinceOrderBean.getOrderIdSet().size());
                            collector.collect(tradeProvinceOrderBean);
                        }
                    }
                });

        SingleOutputStreamOperator<TradeProvinceOrderBean> provinceStream = AsyncDataStream.unorderedWait(reduceStream, new DimAsyncFunction<TradeProvinceOrderBean>() {
            @Override
            public String getId(TradeProvinceOrderBean tradeProvinceOrderBean) {
                return tradeProvinceOrderBean.getProvinceId();
            }

            @Override
            public String getTableName() {
                return "dim_base_province";
            }

            @Override
            public void join(TradeProvinceOrderBean tradeProvinceOrderBean, JSONObject jsonObject) {
                tradeProvinceOrderBean.setProvinceName(jsonObject.getString("name"));
            }
        }, 60, TimeUnit.SECONDS);

        provinceStream.map(new DorisMapFunction<>()).sinkTo(FlinkSinkUtil.getDorisSink(DWS_TRADE_PROVINCE_ORDER_WINDOW));
    }
}












