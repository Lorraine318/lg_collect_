package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * @author liugou
 * @date 2021/5/15 16:33
 */
public class DimSink extends RichSinkFunction<JSONObject> {

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }


}
