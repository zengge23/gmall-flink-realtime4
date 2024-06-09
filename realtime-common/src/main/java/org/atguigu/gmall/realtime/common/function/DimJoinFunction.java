package org.atguigu.gmall.realtime.common.function;

import com.alibaba.fastjson.JSONObject;

public interface DimJoinFunction<T> {

    String getId(T t);
    String getTableName();

    void join(T t, JSONObject jsonObject);

}
