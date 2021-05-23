package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

/**
 * @author liugou
 * @date 2021/5/15 16:33
 *      维度数据的 sink 实现， phoenix
 */
public class DimSink extends RichSinkFunction<JSONObject> {
    //定义phoenix连接对象
    private Connection conn = null;

    @Override
    public void open(Configuration parameters) throws Exception {
            //对连接对象进行初始化
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    //对流中的数据进行处理
    //{"database":"...","xid":5573,"data":{"id":88},"commit":true,"sink_table":"....","type":"insert","table":"...","ts":1612317532}
    @Override
    public void invoke(JSONObject jsonObj, Context context) throws Exception {
        //获取目标表的名称
        final String sink_table = jsonObj.getString("sink_table");
        //需要保留的业务字段
        final JSONObject dataJsonObj = jsonObj.getJSONObject("data");

        //根据data中属性名和属性值，生成upsert语句, 因为phoenix表名大小写会默认大写，则大写转换，以大写为准
        final String upsertSql = getUpsertSql(sink_table.toUpperCase(), dataJsonObj);
            System.out.println("向phoenix 插入数据的sql : " + upsertSql);

         PreparedStatement ps = null;
        try {
            ps = conn.prepareStatement(upsertSql);
            //ps.execute() 返回boolean   ,  ps.executeUpdate()  返回影响条数    主要操作 增删改, 返回类型不一致
            ps.execute();
            //注意：执行完sql插入操作之后，需要手动提交事务。
            conn.commit();
        }catch (SQLException e){
            e.printStackTrace();
            throw new RuntimeException("向phoenix插入数据失败");
        }finally {
            if(ps != null ){
                ps.close();
            }
        }
    }

    //根据data属性和值，生成向phoenix中插入数据的sql语句
    private String getUpsertSql(String tableName, JSONObject dataJsonObj) {


        final Set<String> keys = dataJsonObj.keySet();
        final Collection<Object> values = dataJsonObj.values();

        //"insert into 表空间.表名（列名...） values（值...）"
        String upsertSql = "insert into "+ GmallConfig.HBASE_SCHEMA +"."+tableName+" (" +
                StringUtils.join(keys,",")+ ")";

        String valueSql = " values ('" + StringUtils.join(values,"','") + "')";
        return upsertSql +  valueSql;
    }


}
