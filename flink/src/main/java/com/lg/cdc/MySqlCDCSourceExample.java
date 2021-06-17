package com.lg.cdc;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author liugou.
 * @date 2021/5/17 14:21
 */
public class MySqlCDCSourceExample {

    public static void main(String[] args) throws Exception {

        SourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("")
                .port(3306)
                .databaseList("")
                .username("")
                .password("")
                .deserializer(new StringDebeziumDeserializationSchema())
                .build();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env,settings);
        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);


        tableEnv.createTemporaryView("",env.addSource(sourceFunction));

        Table table = tableEnv.sqlQuery("");



        env.execute("start...");

    }

}
