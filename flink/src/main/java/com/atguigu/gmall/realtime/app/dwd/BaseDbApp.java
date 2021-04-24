package com.atguigu.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.func.TableProcessFunction;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.utils.MyKafkaUtils;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author liugou
 * @date 2021/4/17 16:16
 *
 *          构建业务数据的DWD层
 */
public class BaseDbApp {

    public static void main(String[] args) throws Exception {
        // TODO: 2021/4/17 准备环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //设置CK的参数
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop202:8020/gmall/flink/checkpoint/basedbapp"));


        // TODO: 2021/4/17 获取业务ods层数据
        String topic = "ods_base_db_m";
        String groupId = "base_db_app_group";
        final FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtils.getKafkaSource(topic, groupId);
        final DataStreamSource<String> jsonStrDS = env.addSource(kafkaSource);

        // TODO: 2021/4/17 ETL
        //json转换
       // jsonStrDS.map(jsonStr -> JSON.parseObject(jsonStr));
        final SingleOutputStreamOperator<JSONObject> jsonObjDs = jsonStrDS.map(JSON::parseObject);

        //过滤
        final SingleOutputStreamOperator<JSONObject> filterDs = jsonObjDs.filter(
                jsonobj -> {
                    boolean bo = jsonobj.getString("table") != null && jsonobj.getJSONObject("data") != null && jsonobj.getString("data").length() >= 3;
                    return bo;
                }
        );

        // TODO: 2021/4/24 动态分流  事实表-主流-kafka dwd层  维度表-侧输出流-hbase
        //定义输出到hbase的侧输出流标签
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>(TableProcess.SINK_TYPE_HBASE){};
        //主流写回kafka
        final SingleOutputStreamOperator<JSONObject> kafkaDs = filterDs.process(new TableProcessFunction(hbaseTag));
        //侧输出流，写到hbase
        final DataStream<JSONObject> hbaseDs = kafkaDs.getSideOutput(hbaseTag);

        env.execute("start..");

    }
}
