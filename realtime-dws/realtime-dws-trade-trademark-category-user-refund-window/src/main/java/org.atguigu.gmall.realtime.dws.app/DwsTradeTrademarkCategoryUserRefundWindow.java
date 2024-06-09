package org.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.atguigu.gmall.realtime.common.base.BaseAPP;
import org.atguigu.gmall.realtime.common.bean.TradeTrademarkCategoryUserRefundBean;
import org.atguigu.gmall.realtime.common.function.DimAsyncFunction;
import org.atguigu.gmall.realtime.common.function.DorisMapFunction;
import org.atguigu.gmall.realtime.common.util.DateFormatUtil;
import org.atguigu.gmall.realtime.common.util.FlinkSinkUtil;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

import static org.atguigu.gmall.realtime.common.constant.Constant.DWS_TRADE_TRADEMARK_CATEGORY_USER_REFUND_WINDOW;
import static org.atguigu.gmall.realtime.common.constant.Constant.TOPIC_DWD_TRADE_ORDER_REFUND;

public class DwsTradeTrademarkCategoryUserRefundWindow extends BaseAPP {
    public static void main(String[] args) {
        new DwsTradeTrademarkCategoryUserRefundWindow().start(10031, 1, "dws_trade_trademark_category_user_refund_window", TOPIC_DWD_TRADE_ORDER_REFUND);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {

        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> objStream = stream.flatMap(new FlatMapFunction<String, TradeTrademarkCategoryUserRefundBean>() {
            @Override
            public void flatMap(String s, Collector<TradeTrademarkCategoryUserRefundBean> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    String userId = jsonObject.getString("user_id");
                    String orderId = jsonObject.getString("order_id");
                    String skuId = jsonObject.getString("sku_id");
                    Long ts = jsonObject.getLong("ts") * 1000;
                    if (userId != null && orderId != null && ts != null) {
                        collector.collect(TradeTrademarkCategoryUserRefundBean.builder()
                                .ts(ts)
                                .userId(userId)
                                .orderIdSet(new HashSet<>(Collections.singleton(orderId)))
                                .skuId(skuId)
                                .build());
                    }
                } catch (Exception e) {
                    System.err.println("异常数据 >> " + s);
                }
            }
        });

        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> newObjStream = AsyncDataStream.unorderedWait(objStream, new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>() {
            @Override
            public String getId(TradeTrademarkCategoryUserRefundBean tradeTrademarkCategoryUserRefundBean) {
                return tradeTrademarkCategoryUserRefundBean.getSkuId();
            }

            @Override
            public String getTableName() {
                return "dim_sku_info";
            }

            @Override
            public void join(TradeTrademarkCategoryUserRefundBean tradeTrademarkCategoryUserRefundBean, JSONObject jsonObject) {
                tradeTrademarkCategoryUserRefundBean.setTrademarkId(jsonObject.getString("tm_id"));
                tradeTrademarkCategoryUserRefundBean.setCategory3Id(jsonObject.getString("category3_id"));
            }
        }, 120, TimeUnit.SECONDS);

        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> withWaterMarkStream = newObjStream.assignTimestampsAndWatermarks(WatermarkStrategy.<TradeTrademarkCategoryUserRefundBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L)).withTimestampAssigner(new SerializableTimestampAssigner<TradeTrademarkCategoryUserRefundBean>() {
            @Override
            public long extractTimestamp(TradeTrademarkCategoryUserRefundBean tradeTrademarkCategoryUserRefundBean, long l) {
                return tradeTrademarkCategoryUserRefundBean.getTs();
            }
        }).withIdleness(Duration.ofMinutes(1L)));

        KeyedStream<TradeTrademarkCategoryUserRefundBean, String> keyedStream = withWaterMarkStream.keyBy(new KeySelector<TradeTrademarkCategoryUserRefundBean, String>() {
            @Override
            public String getKey(TradeTrademarkCategoryUserRefundBean tradeTrademarkCategoryUserRefundBean) throws Exception {
                return tradeTrademarkCategoryUserRefundBean.getUserId() + "_" + tradeTrademarkCategoryUserRefundBean.getCategory3Id() + "_" + tradeTrademarkCategoryUserRefundBean.getTrademarkId();
            }
        });

        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> reduceStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5L))).reduce(new ReduceFunction<TradeTrademarkCategoryUserRefundBean>() {
            @Override
            public TradeTrademarkCategoryUserRefundBean reduce(TradeTrademarkCategoryUserRefundBean tradeTrademarkCategoryUserRefundBean, TradeTrademarkCategoryUserRefundBean t1) throws Exception {
                tradeTrademarkCategoryUserRefundBean.getOrderIdSet().addAll(t1.getOrderIdSet());
                return tradeTrademarkCategoryUserRefundBean;
            }
        }, new ProcessWindowFunction<TradeTrademarkCategoryUserRefundBean, TradeTrademarkCategoryUserRefundBean, String, TimeWindow>() {
            @Override
            public void process(String s, ProcessWindowFunction<TradeTrademarkCategoryUserRefundBean, TradeTrademarkCategoryUserRefundBean, String, TimeWindow>.Context context, Iterable<TradeTrademarkCategoryUserRefundBean> iterable, Collector<TradeTrademarkCategoryUserRefundBean> collector) throws Exception {
                TradeTrademarkCategoryUserRefundBean next = iterable.iterator().next();
                TimeWindow window = context.window();
                long start = window.getStart();
                long end = window.getEnd();
                String stt = DateFormatUtil.tsToDateTime(start);
                String ett = DateFormatUtil.tsToDateTime(end);
                String curDt = DateFormatUtil.tsToDateForPartition(System.currentTimeMillis());
                next.setStt(stt);
                next.setEdt(ett);
                next.setCurDate(curDt);
                next.setRefundCount((long) next.getOrderIdSet().size());
                collector.collect(next);
            }
        });

        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> tmStream = AsyncDataStream.unorderedWait(reduceStream, new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>() {
            @Override
            public String getId(TradeTrademarkCategoryUserRefundBean tradeTrademarkCategoryUserRefundBean) {
                return tradeTrademarkCategoryUserRefundBean.getTrademarkId();
            }

            @Override
            public String getTableName() {
                return "dim_base_trademark";
            }

            @Override
            public void join(TradeTrademarkCategoryUserRefundBean tradeTrademarkCategoryUserRefundBean, JSONObject jsonObject) {
                tradeTrademarkCategoryUserRefundBean.setTrademarkName(jsonObject.getString("tm_name"));
            }
        }, 120, TimeUnit.SECONDS);

        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> c3Stream = AsyncDataStream.unorderedWait(tmStream, new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>() {
            @Override
            public String getId(TradeTrademarkCategoryUserRefundBean tradeTrademarkCategoryUserRefundBean) {
                return tradeTrademarkCategoryUserRefundBean.getCategory3Id();
            }

            @Override
            public String getTableName() {
                return "dim_base_category3";
            }

            @Override
            public void join(TradeTrademarkCategoryUserRefundBean tradeTrademarkCategoryUserRefundBean, JSONObject jsonObject) {
                tradeTrademarkCategoryUserRefundBean.setCategory3Name(jsonObject.getString("name"));
                tradeTrademarkCategoryUserRefundBean.setCategory2Id(jsonObject.getString("category2_id"));
            }
        }, 120, TimeUnit.SECONDS);

        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> c2Stream = AsyncDataStream.unorderedWait(c3Stream, new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>() {
            @Override
            public String getId(TradeTrademarkCategoryUserRefundBean tradeTrademarkCategoryUserRefundBean) {
                return tradeTrademarkCategoryUserRefundBean.getCategory2Id();
            }

            @Override
            public String getTableName() {
                return "dim_base_category2";
            }

            @Override
            public void join(TradeTrademarkCategoryUserRefundBean tradeTrademarkCategoryUserRefundBean, JSONObject jsonObject) {
                tradeTrademarkCategoryUserRefundBean.setCategory2Name(jsonObject.getString("name"));
                tradeTrademarkCategoryUserRefundBean.setCategory1Id(jsonObject.getString("category1_id"));
            }
        }, 120, TimeUnit.SECONDS);

        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> c1Stream = AsyncDataStream.unorderedWait(c2Stream, new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>() {
            @Override
            public String getId(TradeTrademarkCategoryUserRefundBean tradeTrademarkCategoryUserRefundBean) {
                return tradeTrademarkCategoryUserRefundBean.getCategory1Id();
            }

            @Override
            public String getTableName() {
                return "dim_base_category1";
            }

            @Override
            public void join(TradeTrademarkCategoryUserRefundBean tradeTrademarkCategoryUserRefundBean, JSONObject jsonObject) {
                tradeTrademarkCategoryUserRefundBean.setCategory1Name(jsonObject.getString("name"));
            }
        }, 120, TimeUnit.SECONDS);

        c1Stream.map(new DorisMapFunction<>()).sinkTo(FlinkSinkUtil.getDorisSink(DWS_TRADE_TRADEMARK_CATEGORY_USER_REFUND_WINDOW));
    }
}
