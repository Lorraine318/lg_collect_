package com.lg.cdc;

import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author liugou.
 * @date 2021/5/21 14:37
 */
public class KafkaCDCSourceExample {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings setting = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, setting);
        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);

        // debezium kafka捕获到变化的数据会自动写入到创建的kafka数据表



    }

}
