package com.atguigu.gmall.realtime.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @author liugou
 * @date 2021/4/4 14:52
 * kafka工具类
 */
public class MyKafkaUtils {

    private static String kafkaServer = "hadoop02:9092,hadoop03:9092,hadoop04:9092";

    //获取flink kafkaConsumer
    public static FlinkKafkaConsumer<String> getKafkaSource(String topic,String groupid) {

            //kafka配置信息
        final Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupid);
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,kafkaServer);


        return new FlinkKafkaConsumer<String>(topic,new SimpleStringSchema(),properties);

    }
    //封装flink生产者
    public static FlinkKafkaProducer<String> getKafkaSink(String topic){
           return  new FlinkKafkaProducer<String>(kafkaServer,topic,new SimpleStringSchema());
    }



}
