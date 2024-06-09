package org.atguigu.gmall.realtime.common.function;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import org.apache.flink.api.common.functions.MapFunction;

public class DorisMapFunction<T> implements MapFunction<T, String> {
    @Override
    public String map(T t) throws Exception {
        SerializeConfig serializeConfig = new SerializeConfig();
        serializeConfig.setPropertyNamingStrategy(PropertyNamingStrategy.SnakeCase);
        String jsonString = JSONObject.toJSONString(t, serializeConfig);
        System.out.println("//>>" + jsonString);
        return jsonString;
    }
}
