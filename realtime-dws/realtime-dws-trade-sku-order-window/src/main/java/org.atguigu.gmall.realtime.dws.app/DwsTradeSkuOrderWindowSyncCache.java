package org.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;
import org.atguigu.gmall.realtime.common.base.BaseAPP;
import org.atguigu.gmall.realtime.common.bean.TradeSkuOrderBean;
import org.atguigu.gmall.realtime.common.util.DateFormatUtil;
import org.atguigu.gmall.realtime.common.util.HBaseUtil;
import org.atguigu.gmall.realtime.common.util.RedisUtil;
import redis.clients.jedis.Jedis;

import java.math.BigDecimal;
import java.time.Duration;

import static org.atguigu.gmall.realtime.common.constant.Constant.HBASE_NAMESPACE;
import static org.atguigu.gmall.realtime.common.constant.Constant.TOPIC_DWD_TRADE_ORDER_DETAIL;

public class DwsTradeSkuOrderWindowSyncCache extends BaseAPP {

    public static void main(String[] args) {
        new DwsTradeSkuOrderWindowSyncCache().start(10029, 1, "dws_trade_sku_order_window", TOPIC_DWD_TRADE_ORDER_DETAIL);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        SingleOutputStreamOperator<JSONObject> jsonObjStream = stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    Long ts = jsonObject.getLong("ts");
                    String id = jsonObject.getString("id");
                    String skuId = jsonObject.getString("sku_id");
                    if (ts != null && id != null && skuId != null) {
                        jsonObject.put("ts", ts * 1000);
                        collector.collect(jsonObject);
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

        SingleOutputStreamOperator<TradeSkuOrderBean> processStream = keyedStream.process(new KeyedProcessFunction<String, JSONObject, TradeSkuOrderBean>() {
            MapState<String, BigDecimal> lastAmountState;

            @Override
            public void open(Configuration parameters) throws Exception {
                MapStateDescriptor<String, BigDecimal> lastAmount = new MapStateDescriptor<>("last_amount", String.class, BigDecimal.class);
                lastAmount.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(30L)).build());
                lastAmountState = getRuntimeContext().getMapState(lastAmount);
            }

            @Override
            public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, TradeSkuOrderBean>.Context context, Collector<TradeSkuOrderBean> collector) throws Exception {
                BigDecimal originalAmount = lastAmountState.get("originalAmount");
                BigDecimal activityReduceAmount = lastAmountState.get("activityReduceAmount");
                BigDecimal couponReduceAmount = lastAmountState.get("couponReduceAmount");
                BigDecimal orderAmount = lastAmountState.get("orderAmount");

                originalAmount = originalAmount == null ? new BigDecimal("0") : originalAmount;
                activityReduceAmount = activityReduceAmount == null ? new BigDecimal("0") : activityReduceAmount;
                couponReduceAmount = couponReduceAmount == null ? new BigDecimal("0") : couponReduceAmount;
                orderAmount = orderAmount == null ? new BigDecimal("0") : orderAmount;

                BigDecimal curOriginalAmount = jsonObject.getBigDecimal("order_price").multiply(jsonObject.getBigDecimal("sku_num"));

                TradeSkuOrderBean tradeSkuOrderBean = TradeSkuOrderBean.builder()
                        .skuId(jsonObject.getString("sku_id"))
                        .orderDetailId(jsonObject.getString("id"))
                        .ts(jsonObject.getLong("ts"))
                        .originalAmount(curOriginalAmount.subtract(originalAmount))
                        .orderAmount(jsonObject.getBigDecimal("split_total_amount").subtract(orderAmount))
                        .activityReduceAmount(jsonObject.getBigDecimal("split_activity_amount").subtract(activityReduceAmount))
                        .couponReduceAmount(jsonObject.getBigDecimal("split_coupon_amount").subtract(couponReduceAmount))
                        .build();

                lastAmountState.put("curOriginalAmount", curOriginalAmount);
                lastAmountState.put("activityReduceAmount", jsonObject.getBigDecimal("split_activity_amount"));
                lastAmountState.put("couponReduceAmount", jsonObject.getBigDecimal("split_coupon_amount"));
                lastAmountState.put("orderAmount", jsonObject.getBigDecimal("split_total_amount"));

                collector.collect(tradeSkuOrderBean);
            }
        });

        SingleOutputStreamOperator<TradeSkuOrderBean> reduceStream = processStream.keyBy(new KeySelector<TradeSkuOrderBean, String>() {
                    @Override
                    public String getKey(TradeSkuOrderBean tradeSkuOrderBean) throws Exception {
                        return tradeSkuOrderBean.getSkuId();
                    }
                }).window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)))
                .reduce(new ReduceFunction<TradeSkuOrderBean>() {
                    @Override
                    public TradeSkuOrderBean reduce(TradeSkuOrderBean tradeSkuOrderBean, TradeSkuOrderBean t1) throws Exception {
                        tradeSkuOrderBean.setOriginalAmount(tradeSkuOrderBean.getOriginalAmount().add(t1.getOriginalAmount()));
                        tradeSkuOrderBean.setCouponReduceAmount(tradeSkuOrderBean.getCouponReduceAmount().add(t1.getCouponReduceAmount()));
                        tradeSkuOrderBean.setActivityReduceAmount(tradeSkuOrderBean.getActivityReduceAmount().add(t1.getActivityReduceAmount()));
                        tradeSkuOrderBean.setOrderAmount(tradeSkuOrderBean.getOrderAmount().add(t1.getOrderAmount()));
                        return tradeSkuOrderBean;
                    }
                }, new ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>.Context context, Iterable<TradeSkuOrderBean> iterable, Collector<TradeSkuOrderBean> collector) throws Exception {
                        TimeWindow window = context.window();
                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        String curDt = DateFormatUtil.tsToDateForPartition(System.currentTimeMillis());
                        for (TradeSkuOrderBean tradeSkuOrderBean : iterable) {
                            tradeSkuOrderBean.setStt(stt);
                            tradeSkuOrderBean.setEdt(edt);
                            tradeSkuOrderBean.setCurDate(curDt);
                            collector.collect(tradeSkuOrderBean);
                        }
                    }
                });

        reduceStream.map(new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
            Connection connection;
            Jedis jedis;

            @Override
            public void open(Configuration parameters) throws Exception {
                connection = HBaseUtil.getConnection();
                jedis = RedisUtil.getJedis();
            }

            @Override
            public void close() throws Exception {
                HBaseUtil.closeConnection(connection);
                RedisUtil.closeJedis(jedis);
            }

            @Override
            public TradeSkuOrderBean map(TradeSkuOrderBean tradeSkuOrderBean) throws Exception {
                String redisKey = RedisUtil.getKey("dim_sku_info", tradeSkuOrderBean.getSkuId());
                String dim = jedis.get(redisKey);
                JSONObject dimSkuInfo = new JSONObject();
                if (dim == null | dim.isEmpty()){
                    dimSkuInfo = HBaseUtil.getCells(connection,HBASE_NAMESPACE,"dim_sku_info",tradeSkuOrderBean.getSkuId());
                    if (dimSkuInfo.size() != 0){
                        jedis.setex(redisKey,24*60*60, dimSkuInfo.toJSONString());
                    }
                }else {
                    dimSkuInfo = JSONObject.parseObject(dim);
                }

                if (dimSkuInfo.size() != 0){
                    tradeSkuOrderBean.setCategory3Id(dimSkuInfo.getString("category3_id"));
                    tradeSkuOrderBean.setTrademarkId(dimSkuInfo.getString("tm_id"));
                    tradeSkuOrderBean.setSpuId(dimSkuInfo.getString("spu_id"));
                    tradeSkuOrderBean.setSkuName(dimSkuInfo.getString("sku_name"));
                }else {
                    System.out.println("没有对应的维度信息:" + tradeSkuOrderBean);
                }
                return tradeSkuOrderBean;
            }
        });

//        SingleOutputStreamOperator<TradeSkuOrderBean> fullDimStream = reduceStream.map(new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
//            @Override
//            public void close() throws Exception {
//                HBaseUtil.closeConnection(connection);
//            }
//
//            Connection connection;
//
//            @Override
//            public void open(Configuration parameters) throws Exception {
//                connection = HBaseUtil.getConnection();
//            }
//
//            @Override
//            public TradeSkuOrderBean map(TradeSkuOrderBean tradeSkuOrderBean) throws Exception {
//                JSONObject dimSkuInfo = HBaseUtil.getCells(connection, HBASE_NAMESPACE, "dim_sku_info", tradeSkuOrderBean.getSkuId());
//                tradeSkuOrderBean.setCategory3Id(dimSkuInfo.getString("category3_id"));
//                tradeSkuOrderBean.setTrademarkId(dimSkuInfo.getString("tm_id"));
//                tradeSkuOrderBean.setSpuId(dimSkuInfo.getString("spu_id"));
//                tradeSkuOrderBean.setSkuName(dimSkuInfo.getString("sku_name"));
//
//                JSONObject dimSpuInfo = HBaseUtil.getCells(connection, HBASE_NAMESPACE, "dim_spu_info", tradeSkuOrderBean.getSpuId());
//                tradeSkuOrderBean.setSpuName(dimSpuInfo.getString("spu_name"));
//                tradeSkuOrderBean.setCategory3Id(dimSpuInfo.getString("category3_id"));
//
//                JSONObject dimBaseCategory3 = HBaseUtil.getCells(connection, HBASE_NAMESPACE, "dim_base_category3", tradeSkuOrderBean.getCategory3Id());
//                tradeSkuOrderBean.setCategory2Id(dimBaseCategory3.getString("category2_id"));
//                tradeSkuOrderBean.setCategory3Name(dimBaseCategory3.getString("name"));
//
//                JSONObject dimBaseCategory2 = HBaseUtil.getCells(connection, HBASE_NAMESPACE, "dim_base_category2", tradeSkuOrderBean.getCategory2Id());
//                tradeSkuOrderBean.setCategory2Name(dimBaseCategory2.getString("name"));
//                tradeSkuOrderBean.setCategory1Id(dimBaseCategory2.getString("category1_id"));
//
//                JSONObject dimBaseCategory1 = HBaseUtil.getCells(connection, HBASE_NAMESPACE, "dim_base_category1", tradeSkuOrderBean.getCategory1Id());
//                tradeSkuOrderBean.setCategory1Name(dimBaseCategory1.getString("name"));
//
//                JSONObject dimBaseTrademark = HBaseUtil.getCells(connection, HBASE_NAMESPACE, "dim_base_trademark", tradeSkuOrderBean.getTrademarkId());
//                tradeSkuOrderBean.setTrademarkName(dimBaseTrademark.getString("tm_name"));
//
//                return tradeSkuOrderBean;
//            }
//        });

//        fullDimStream.map(new DorisMapFunction<>()).sinkTo(FlinkSinkUtil.getDorisSink(DWS_TRADE_SKU_ORDER_WINDOW));
    }
}


















