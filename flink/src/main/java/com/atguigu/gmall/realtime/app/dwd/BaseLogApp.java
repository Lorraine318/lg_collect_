package com.atguigu.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author liugou
 * @date 2021/4/4 14:49
 * <p>
 *          获取ods层日志数据， 构建日志数据的dwd层
 */
public class BaseLogApp {

    // TODO: 2021/4/17 设置topic
    private static final String TOPIC_START = "dwd_start_log";
    private static final String TOPIC_DISPLAY = "dwd_display_log";
    private static final String TOPIC_PAGE = "dwd_page_log";

    public static void main(String[] args) throws Exception {

        // TODO: 1. 准备环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // TODO: 1.1 设置并行度  和kafka分区并行度相同就好
        env.setParallelism(4);

        // TODO: 2. 保证exactly-once
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new RocksDBStateBackend("hdfs://hadoop02:8020/gmail/checkpoint/baselogApp"));

        //本地测试  用户权限问题
        System.setProperty("HADOOP_USER_NAME","liurunfeng");

        // TODO: 3. 从kafka获取数据
        String topic = "ods_base_log";
        String groupid = "";
        final DataStreamSource<String> kafkaDs = env.addSource(MyKafkaUtils.getKafkaSource(topic, groupid));

        // 对ods层的日志数据进行格式转换  String -> json
        final SingleOutputStreamOperator<JSONObject> jsonObjDs = kafkaDs.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String value) throws Exception {
                final JSONObject jsonObject = JSON.parseObject(value);
                return jsonObject;
            }
        });
       // jsonObjDs.print("json>>>>>>>>>>>>>>>");

        /**
         *
         *     TODO: 4 识别新老访客
         *     保存mid某天访问情况（将首次访问日期作为状态保存起来，等后面该设备在有日志过来的时候，从状态中获取日期
         *     和日志产生日志进行对比，如果状态不为空，并且状态日期和当前日期不相等说明是老访客，如果is_new是1，那么对齐状态进行修复）
         */

        // TODO:  4.1 根据mid日志进行分组
        final KeyedStream<JSONObject, Object> keyedStream = jsonObjDs.keyBy(
                data -> {
                    return data.getJSONObject("common").getString("mid");
                }
        );
        // TODO:  4.2 新老访客状态修复    针对 key做状态还是针对算子做状态,这里我们根据key做状态更合适。
        final SingleOutputStreamOperator<JSONObject> jsonDSWithFlag = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {

            //定义状态
            private ValueState<String> firstVisitDateState;
            //因为日期是时间戳，定义日期格式化对象
            private SimpleDateFormat sdf;

            @Override
            public void open(Configuration parameters) throws Exception {
                //对状态初始化
                firstVisitDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("newMidDateState", String.class));
                sdf = new SimpleDateFormat("yyyyMMdd");

            }

            @Override
            public JSONObject map(JSONObject jsonObj) throws Exception {

                //获取当前日志标记状态
                String isNew = jsonObj.getJSONObject("common").getString("is_new");

                //获取日志时间
                final Long ts = jsonObj.getLong("ts");


                //如果是1，表示前端设置的是新用户，需要判断是否准确并进行修复
                if ("1".equals(isNew)) {
                    //获取当前mid对象的状态
                    final String stateDate = firstVisitDateState.value();
                    //对当前日志的日期进行转换
                    final String curDate = sdf.format(new Date(ts));
                    //如果状态不为空，并且状态日期和当前日期不相等，说明是老访客
                    if (stateDate != null && stateDate.length() != 0) {
                        //只要是当天的都可以任务是新用户。
                        //判断是否为同一天数据
                        if (!stateDate.equals(curDate)) {
                            isNew = "0";
                            jsonObj.getJSONObject("common").put("is_new", isNew);   //更新 状态值。
                        }
                    } else {
                        //如果还没记录设备的状态，将当前访问日期作为状态值
                        firstVisitDateState.update(curDate);    //表示新用户，记录日期
                    }
                }

                return null;
            }
        });

        // TODO: 2021/4/17 分流： 根据日志数据内容，将日志数据分为3类，页面日志，   启动日志和     曝光日志。
        //页面日志输出到主流，启动日志输出到启动侧输出流，曝光日志输出到曝光日志侧输出流

        //定义启动侧输出流   json中有start
        final OutputTag<String> startTag = new OutputTag<String>("start"){};
        //定义曝光侧输出流   json中有display
        final OutputTag<String> displayTag = new OutputTag<String>("display"){};

        //侧输出流
        final SingleOutputStreamOperator<String> pageDS = jsonDSWithFlag.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObject, Context context, Collector<String> collector) throws Exception {
                //获取启动日志标记  start
                final JSONObject startJsonObject = jsonObject.getJSONObject("start");
                //将json格式转换为字符串，方便向侧输出流输出以及向kafka写入
                String dataStr = jsonObject.toString();

                //判断是否为启动日志
                if (startJsonObject != null && startJsonObject.size() > 0) {
                    //如果是启动日志，输出到启动侧输出流
                    context.output(startTag, dataStr);
                } else {
                    //如果不是启动日志
                    final JSONArray displays = jsonObject.getJSONArray("displays");
                    if (displays != null && displays.size() > 0) {
                        //如果是曝光日志，遍历输出到启动侧输出流
                        for (int i = 0; i < displays.size(); i++) {
                            //获取每条曝光数据
                            final JSONObject dataobj = displays.getJSONObject(i);
                            //为每条曝光时间增加pageid字段。
                            final String page_id = jsonObject.getJSONObject("page").getString("page_id");
                            dataobj.put("page_id", page_id);
                            context.output(displayTag, dataobj.toString());
                        }
                    } else {
                        //主流，页面日志
                        collector.collect(dataStr);

                    }
                }
            }
        });

        //获取侧输出流
        final DataStream<String> startDs = pageDS.getSideOutput(startTag);
        final DataStream<String> displayDs = pageDS.getSideOutput(displayTag);
        pageDS.print("pageDag---");
        startDs.print("startdag---");
        displayDs.print("displayDag--");


        // TODO: 2021/4/17  将不同流的数据写回到kafka的不同topic中
        final FlinkKafkaProducer<String> startSink = MyKafkaUtils.getKafkaSink(TOPIC_START);
        startDs.addSink(startSink);
        final FlinkKafkaProducer<String> displaySink = MyKafkaUtils.getKafkaSink(TOPIC_DISPLAY);
        displayDs.addSink(displaySink);
        final FlinkKafkaProducer<String> pageSink = MyKafkaUtils.getKafkaSink(TOPIC_PAGE);
        pageDS.addSink(pageSink);
        
        env.execute("start..");
    }
}
