package com.atguigu.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.func.DimSink;
import com.atguigu.gmall.realtime.app.func.TableProcessFunction;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.utils.MyKafkaUtils;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.hadoop.hdfs.ReadStatistics;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * @author liugou
 * @date 2021/4/17 16:16
 *
 *          获取ods层业务数据，构建业务数据的DWD层
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

        //todo
        // 重启策略， 如果说没有开启checkpoint，那么重启策略就是norestart
        // 如果开启checkpoint,那么重启策略会尝试自动帮你进行重启，重启Intger.maxvalue
        //如果没有设置ck,切代码中出现异常，则默认不会帮我们重启
        //如果设置了ck,且代码中出现异常，则默认帮我们重启，重启为Integer.maxvalue
        //可以通过下面方式开启对应的重启模式，不设置则按上面规则走
        env.setRestartStrategy(RestartStrategies.noRestart());//设置不重启，表示代码异常就退出任务了

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

        kafkaDs.print("事实>>>");
        hbaseDs.print("维度>>>");
        // TODO:  hbase sink
        hbaseDs.addSink(new DimSink());
        // TODO:  kafka sink
        //为什么需要自定义序列化：因为要发往不同的topic,每条数据只有收到的时候才能确定topic的存在，那么在序列化创建ProducerRecord的时候就可以传入topic。
        final FlinkKafkaProducer<JSONObject> kafkaSink = MyKafkaUtils.getKafkaSlinkSchema(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public void open(SerializationSchema.InitializationContext context) throws Exception {
                System.out.println("kafka序列化");
            }
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObject, @Nullable Long aLong) {
                final String sinkTopic = jsonObject.getString("sink_table");
                final JSONObject data = jsonObject.getJSONObject("data");
                return new ProducerRecord<>(sinkTopic, data.toString().getBytes());
            }
        });
        kafkaDs.addSink(kafkaSink);
        // TODO:  业务事实数据 分向不同主题
       // kafkaDs.addSink();
        env.execute("start..");

    }
}
