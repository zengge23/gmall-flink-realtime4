package org.atguigu.gmall.realtime.dwd.db.split.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.atguigu.gmall.realtime.common.base.BaseAPP;
import org.atguigu.gmall.realtime.common.util.DateFormatUtil;
import org.atguigu.gmall.realtime.common.util.FlinkSinkUtil;

import java.time.Duration;

import static org.atguigu.gmall.realtime.common.constant.Constant.*;

public class DwdBaseLog extends BaseAPP {

    public static void main(String[] args) {
        new DwdBaseLog().start(10011, 4, "dwd_base_log", TOPIC_LOG);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        SingleOutputStreamOperator<JSONObject> jsonObjStream = stream.flatMap(new FlatMapFunction<String, JSONObject>() {

            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    JSONObject page = jsonObject.getJSONObject("page");
                    JSONObject start = jsonObject.getJSONObject("start");
                    JSONObject common = jsonObject.getJSONObject("common");
                    Long ts = jsonObject.getLong("ts");
                    if (page != null || start != null) {
                        if (common != null && common.getString("mid") != null && ts != null) {
                            collector.collect(jsonObject);
                        }
                    }
                } catch (Exception e) {
                    System.out.println("异常数据: " + s);
                }
            }
        });

        KeyedStream<JSONObject, String> keyedStream = jsonObjStream.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject jsonObject, long l) {
                return jsonObject.getLong("ts");
            }
        })).keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObject) throws Exception {
                return jsonObject.getJSONObject("common").getString("mid");
            }
        });

        SingleOutputStreamOperator<JSONObject> isNewFixDS = keyedStream.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {

            ValueState<String> firstLoginDtState;

            @Override
            public void open(Configuration parameters) throws Exception {
                firstLoginDtState = getRuntimeContext().getState(new ValueStateDescriptor<String>("first_login_dt_state", String.class));
            }

            @Override
            public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                JSONObject common = jsonObject.getJSONObject("common");
                String isNew = common.getString("is_new");
                String firstLoginDt = firstLoginDtState.value();
                Long ts = jsonObject.getLong("ts");
                String curDt = DateFormatUtil.tsToDate(ts);
                if ("1".equals(isNew)) {
                    if (firstLoginDt != null && !firstLoginDt.equals(curDt)) {
                        //如果状态不为空 日期也不是今天 说明当前数据有问题 不是新访客或旧访客伪装新访客
                        common.put("is_new", "0");
                    } else if (firstLoginDt == null) {
                        firstLoginDtState.update(curDt);
                    } else {

                    }
                } else {
                    if (firstLoginDt == null) {
                        //老用户 flink实时数仓里面没有记录这个访客 需要补充访客的信息
                        //把访客首次登录日期补充一个值，今天以前的任意一天都可以使用昨天的日期
                        firstLoginDtState.update(DateFormatUtil.tsToDate(ts - 24 * 60 * 60 * 1000L));
                    } else {

                    }
                }
                collector.collect(jsonObject);
            }
        });

        OutputTag<String> startTag = new OutputTag<>("start", TypeInformation.of(String.class));
        OutputTag<String> errorTag = new OutputTag<>("err", TypeInformation.of(String.class));
        OutputTag<String> displayTag = new OutputTag<String>("display") {
        };
        OutputTag<String> actionTag = new OutputTag<>("action", TypeInformation.of(String.class));
        SingleOutputStreamOperator<String> pageDS = isNewFixDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, String>.Context context, Collector<String> collector) throws Exception {
                JSONObject err = jsonObject.getJSONObject("err");
                if (err != null) {
                    context.output(errorTag, err.toJSONString());
                    jsonObject.remove("err");
                }
                JSONObject page = jsonObject.getJSONObject("page");
                JSONObject start = jsonObject.getJSONObject("start");
                JSONObject common = jsonObject.getJSONObject("common");
                Long ts = jsonObject.getLong("ts");
                if (start != null) {
                    context.output(startTag, jsonObject.toJSONString());
                } else if (page != null) {
                    JSONArray displays = jsonObject.getJSONArray("displays");
                    if (displays != null) {
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            display.put("common", common);
                            display.put("ts", ts);
                            display.put("page", page);
                            context.output(displayTag, display.toJSONString());
                        }
                        jsonObject.remove("displays");
                    }

                    JSONArray actions = jsonObject.getJSONArray("actions");
                    if (actions != null) {
                        for (int i = 0; i < actions.size(); i++) {
                            JSONObject action = actions.getJSONObject(i);
                            action.put("common", common);
                            action.put("ts", ts);
                            action.put("page", page);
                            context.output(actionTag, action.toJSONString());
                        }
                        jsonObject.remove("actions");
                    }
                    collector.collect(jsonObject.toJSONString());
                }
            }
        });

        SideOutputDataStream<String> startDS = pageDS.getSideOutput(startTag);
        SideOutputDataStream<String> errorDS = pageDS.getSideOutput(errorTag);
        SideOutputDataStream<String> displayDS = pageDS.getSideOutput(displayTag);
        SideOutputDataStream<String> actionDS = pageDS.getSideOutput(actionTag);

        pageDS.print("page -> ");
//        startDS.print("start ->> ");
//        errorDS.print("error ->>> ");
//        displayDS.print("display ->>>> ");
//        actionDS.print("action ->>>>> ");

        pageDS.sinkTo(FlinkSinkUtil.getKafkaSink(TOPIC_DWD_TRAFFIC_PAGE));
        startDS.sinkTo(FlinkSinkUtil.getKafkaSink(TOPIC_DWD_TRAFFIC_START));
        errorDS.sinkTo(FlinkSinkUtil.getKafkaSink(TOPIC_DWD_TRAFFIC_ERR));
        displayDS.sinkTo(FlinkSinkUtil.getKafkaSink(TOPIC_DWD_TRAFFIC_DISPLAY));
        actionDS.sinkTo(FlinkSinkUtil.getKafkaSink(TOPIC_DWD_TRAFFIC_ACTION));
    }
}



































