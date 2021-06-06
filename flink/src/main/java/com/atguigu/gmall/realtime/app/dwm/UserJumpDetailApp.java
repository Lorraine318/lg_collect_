package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.*;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @author liugou
 * @date 2021/6/6 14:26
 *
 * DWM - 跳出明细
 *
 * 需要具备：
 * 1.刚页面是否有上个页面last_page_id 来判断是否是访问的第一个页面。
 * 2.访问很长一段时间，用户没有再继续访问其他页面。    通过CEP完成。
 */
public class UserJumpDetailApp {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop:8020/gmall/checkpoint/userJumpDetail"));

        String sourceTopic = "dwd_page_log";
        String groupid = "user_jump_detail_group";
        String sinkTopic = "dwm_user_jump_detail";

        final FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtils.getKafkaSource(sourceTopic, groupid);
        final DataStreamSource<String> jsonStrDs = env.addSource(kafkaSource);

        final SingleOutputStreamOperator<JSONObject> jsonObjDs = jsonStrDs.map(jsonStr -> JSON.parseObject(jsonStr));

        //todo: 1.12之后不需要了,默认时间语义就是事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //todo 但是指定事件时间字段需要,通过watermark来指定。
        final SingleOutputStreamOperator<JSONObject> jsonObjWithTSDs = jsonObjDs.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofMillis(3000)).withTimestampAssigner(
                new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject jsonObject, long l) {
                        return jsonObject.getLong("ts'");
                    }
                }
        ));

        jsonObjDs.print(">>>>>>>>>>>>");

        final KeyedStream<JSONObject, String> keyByMidDs = jsonObjWithTSDs.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        // TODO: 2021/6/6  配置CEP表达式
        final Pattern<JSONObject, JSONObject> within = Pattern.<JSONObject>begin("first")
                .where(
                        new SimpleCondition<JSONObject>() {
                            @Override
                            public boolean filter(JSONObject jsonObject) throws Exception {
                                String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");

                                if (lastPageId == null || lastPageId.length() == 0) {
                                    return true;
                                }
                                return false;
                            }
                        })
                .next("next")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        String pageId = jsonObject.getJSONObject("page").getString("page_id");
                        if (pageId == null || pageId.length() == 0) {
                            return true;
                        }
                        return false;
                    }
                })
                .within(Time.milliseconds(10000));

        //根据CEP表达式筛选流
        final PatternStream<JSONObject> pattern = CEP.pattern(keyByMidDs, within);

        //从赛选之后的流中提取数据，将超时数据，放到测输出流中
        final OutputTag<String> timeout = new OutputTag<>("timeout");
        //select 是以返回值向下输出
        //
//        pattern.select(
//                new PatternSelectFunction<JSONObject, String>() {
//                    @Override
//                    public String select(Map<String, List<JSONObject>> map) throws Exception {
//                        return null;
//                    }
//                }
//        )

        //flatSelect  是以状态向下输出
        final SingleOutputStreamOperator<Object> firstDs = pattern.flatSelect(timeout,
                new PatternFlatTimeoutFunction<JSONObject, String>() {
                    @Override
                    public void timeout(Map<String, List<JSONObject>> map, long l, Collector<String> collector) throws Exception {
                        //获取符合first的json对象
                        final List<JSONObject> first = map.get("first");
                        for (JSONObject jsonObject : first) {
                            //向OutPutTag输出数据
                            collector.collect(jsonObject.toJSONString());
                        }
                    }
                },
                //处理没有超时的数据
                new PatternFlatSelectFunction<JSONObject, Object>() {
                    @Override
                    public void flatSelect(Map<String, List<JSONObject>> map, Collector<Object> collector) throws Exception {
                        //没有超时数据不在我们统计范围之内，所以不需要写
                    }
                }
        );
        //从侧输出流中获取超时数据
        final DataStream<String> jumpDs = firstDs.getSideOutput(timeout);
        jumpDs.addSink(MyKafkaUtils.getKafkaSink(sinkTopic));


        env.execute("..");
    }

}
