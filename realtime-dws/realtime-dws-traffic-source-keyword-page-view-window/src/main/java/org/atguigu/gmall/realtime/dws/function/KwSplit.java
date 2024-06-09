package org.atguigu.gmall.realtime.dws.function;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.atguigu.gmall.realtime.dws.util.IkUtil;

import java.util.Set;

@FunctionHint(output = @DataTypeHint("ROW<keyword STRING, length INT>"))
public class KwSplit extends TableFunction<Row> {

    public void eval(String keywords) {
        Set<String> split = IkUtil.split(keywords);
        for (String s : split) {
            collect(Row.of(s, s.length()));
        }
    }
}










