package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtils;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author liugou
 * @date 2021/6/5 15:29
 *
 * DWM -uv明细
 * 独立访客uv表示当日的访客数。
 * 需要具备：
 * 1.必须是访客打开的第一个页面（可根据日志中的last_page 来判断上个跳转过来的 页面是空，则为访客 打开的第一个页面），
 * 2.且：访客可能多次进入，需要进行去重。
 */
public class UniqueVisitApp {

    /**
     * 流程：
     * 1.从kafka dwd_page_log 层读取数据
     * 2.过滤得到独立访客
     * 3.写到kafka DWM层
     *
     */
    public static void main(String[] args) throws Exception {

       // final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        //设置并行度
        env.setParallelism(4);

        //设置检查点
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop002:8020/gmall/checkpoint/uniquevisit"));

        // TODO: 从kafka dwd_page_log 主题接收数据
        String sourceTopic = "dwd_page_log";
        String groupid = "uniqueVisit_app_group";
        String sinkTopic = "dwm_unique_visit";
        final FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtils.getKafkaSource(sourceTopic, groupid);
        final DataStreamSource<String> jsonStrDs = env.addSource(kafkaSource);


        //todo 对读取到的数据进行结构的转换
        final SingleOutputStreamOperator<JSONObject> jsonObjDs = jsonStrDs.map(jsonStr -> JSON.parseObject(jsonStr));

        jsonObjDs.print(">>>>>>>>>>>>>>");

        // TODO: 2021/6/5 对设备进行分组
        final KeyedStream<JSONObject, String> keyByWithMidDs = jsonObjDs.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        // TODO: 过滤得到UV
        final SingleOutputStreamOperator<JSONObject> filter = keyByWithMidDs.filter(new RichFilterFunction<JSONObject>() {
            //定义状态
            ValueState<String> lastVisitState = null;
            //定义日期工具类
            SimpleDateFormat sdf = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                //初始化状态以及日期工具类
                sdf = new SimpleDateFormat("yyyyMMdd");
                final ValueStateDescriptor<String> lastVisitState = new ValueStateDescriptor<>("lastVisitState", String.class);
                //因为统计的是日活，所以状态数据只在当天有效，过了一天就可以失效
                lastVisitState.enableTimeToLive(
                        StateTtlConfig
                                .newBuilder(Time.days(1))
                                .build()
                );
                this.lastVisitState = getRuntimeContext().getState(
                        lastVisitState);
            }

            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                //首先判断当前页面是否是从别的页面跳转过啦ide
                final String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                if (lastPageId != null && lastPageId.length() > 0) {
                    return false;
                }
                final Long ts = jsonObject.getLong("ts");
                final String longDate = sdf.format(new Date(ts));
                //用当前页面的访问时间和状态时间进行对比
                final String lastValue = lastVisitState.value();
                if (lastVisitState != null && lastValue.length() > 0 && lastVisitState.equals(longDate)) {
                    System.out.println("以访问： lastVisitDate - " + lastValue + ",|| longDate: " + longDate);
                    return false;
                } else {
                    System.out.println("未访问： lastVisitDate - " + lastValue + ",|| longDate: " + longDate);
                    lastVisitState.update(longDate);
                    return true;
                }
            }
        });

        final SingleOutputStreamOperator<String> kafkaDs = filter.map(jsonObj -> jsonObj.toJSONString());

        kafkaDs.addSink(MyKafkaUtils.getKafkaSink(sinkTopic));

        env.execute("...");
    }

}
