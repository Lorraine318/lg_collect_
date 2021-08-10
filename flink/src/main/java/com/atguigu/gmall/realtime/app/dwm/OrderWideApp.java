package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall.realtime.bean.OrderDetail;
import com.atguigu.gmall.realtime.bean.OrderInfo;
import com.atguigu.gmall.realtime.bean.OrderWide;
import com.atguigu.gmall.realtime.utils.MyKafkaUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;

/**
 * @author liugou
 * @date 2021/6/6 16:58
 *
 * DWM - 订单拉宽表
 *      1.订单表   2. 订单明细表
 *
 */
public class OrderWideApp {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop:8020/gmall/checkpoint/userJumpDetail"));

        //从kafka 的 dwd 层读取订单和订单明细数据,lakuan
        String orderInfoSourceTopice = "dwd_order_info";
        String orderDetailSourceTopice = "dwd_order_detail";
        String orderWideSinkTopice = "dwm_order_wide";
        String groupId = "order_wide_group";

        // TODO: 读取订单数据
        final FlinkKafkaConsumer<String> orderInfoSouceDs = MyKafkaUtils.getKafkaSource(orderInfoSourceTopice, groupId);
        final DataStreamSource<String> orderInfoJsonStrDs = env.addSource(orderInfoSouceDs);

        // TODO: 读取订单明细
        final FlinkKafkaConsumer<String> orderDetailSouce = MyKafkaUtils.getKafkaSource(orderDetailSourceTopice, groupId);
        final DataStreamSource<String> orderDetailJsonStrDs = env.addSource(orderDetailSouce);

        // TODO: 订单 对数据进行结构转换
        final SingleOutputStreamOperator<OrderInfo> orderInfoDs = orderInfoJsonStrDs.map(new RichMapFunction<String, OrderInfo>() {
            SimpleDateFormat sdf = null;
            @Override
            public void open(Configuration parameters) throws Exception { sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); }
            @Override
            public OrderInfo map(String s) throws Exception {
                final OrderInfo orderInfo = JSON.parseObject(s, OrderInfo.class);
                orderInfo.setCreate_ts(sdf.parse(orderInfo.getCreate_time()).getTime());
                return orderInfo;
            }
        });


        // TODO:  转换订单明细
        final SingleOutputStreamOperator<OrderDetail> orderDetailDs = orderDetailJsonStrDs.map(new RichMapFunction<String, OrderDetail>() {
            SimpleDateFormat sdf = null;
            @Override
            public void open(Configuration parameters) throws Exception { sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); }
            @Override
            public OrderDetail map(String s) throws Exception {
                final OrderDetail orderDetail = JSON.parseObject(s, OrderDetail.class);
                orderDetail.setCreate_ts(sdf.parse(orderDetail.getCreate_time()).getTime());
                return orderDetail;
            }
        });

        orderInfoDs.print(">>>>>>>");
        orderDetailDs.print(">>>>>");



        //双流join   inteval join
        // TODO:   指定事件时间字段
        //1.订单指定事件时间字段
        final SingleOutputStreamOperator<OrderInfo> orderInfoWithTsDs = orderInfoDs.assignTimestampsAndWatermarks(
                WatermarkStrategy.
                        <OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                            @Override
                            public long extractTimestamp(OrderInfo orderInfo, long l) {
                                return orderInfo.getCreate_ts();
                            }
                        })
        );
        //2.订单明细指定事件时间字段
        final SingleOutputStreamOperator<OrderDetail> orderDetailWithTsDs = orderDetailDs.assignTimestampsAndWatermarks(
                WatermarkStrategy.<OrderDetail>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                            @Override
                            public long extractTimestamp(OrderDetail orderDetail, long l) {
                                return orderDetail.getCreate_ts();
                            }
                        })
        );

        //TODO: 分组 指定关联key
        final KeyedStream<OrderInfo, Long> orderInfoKeyedDs = orderInfoWithTsDs.keyBy(OrderInfo::getId);
        final KeyedStream<OrderDetail, Long> orderDetailKeyedDs = orderDetailWithTsDs.keyBy(OrderDetail::getOrder_id);

        // TODO: 双流join
        final SingleOutputStreamOperator<OrderWide> orderWideDs = orderInfoKeyedDs
                .intervalJoin(orderDetailKeyedDs)
                .between(Time.milliseconds(-5), Time.milliseconds(5)) //向前匹配5秒，向后匹配5秒
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, Context context, Collector<OrderWide> collector) throws Exception {
                        collector.collect(new OrderWide(orderInfo, orderDetail));
                    }
                });

        orderWideDs.print(">>>>> order wide");




        env.execute("..");

    }

}
