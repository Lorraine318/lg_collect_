package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.common.GmallConfig;
import com.atguigu.gmall.realtime.utils.MySqlUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

/**
 * @author liugou
 * @date 2021/4/24 15:31
 *      分流函数处理 , 初始化读取配置表并定时更新 (有两点不足 1.可以使用flink 的 onTimer进行定时更新。 2. 可以缓存建表，批量构建)
 */
public class TableProcessFunction  extends ProcessFunction<JSONObject, JSONObject> {

    private OutputTag<JSONObject> outputTag;
    //用于在内存中存放配置信息中的表名_操作，和相关数据内容，
    private Map<String,TableProcess> tableProcessMap = new HashMap<>();
    //用于在内存中存放已经在hbase建过的表（在phoenix中已经建过的表）
    private Set<String> existsTables = new HashSet<>();
    //phoenix连接对象
    Connection conn;

    public TableProcessFunction(OutputTag outputTag){
        this.outputTag = outputTag;
    }

    //在函数被调用的时候,执行一次。
    @Override
    public void open(Configuration parameters) throws Exception {
        //初始化phoenix连接
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        //初始化配置信息
        refreshMeta();
        //因为配置表的数据可能发生变化，需要每隔一段时间从配置表中查询一次数据
        final Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                refreshMeta();
            }
        },5000,5000); //延时delay毫秒之后，每个period秒执行一次

    }
    //每条元素执行一次,进行分流处理，主要根据内存中配置表map对当前进来的元素进行分流
    @Override
    public void processElement(JSONObject jsonObject, Context context, Collector<JSONObject> collector) throws Exception {

    }

    private   void refreshMeta(){
        System.out.println("查询配置表信息");
        List<TableProcess> tableProcessList = MySqlUtils.queryList("select * from table_process", TableProcess.class,true);
        //对查询出来的结果集遍历
        for (TableProcess tableProcess:
             tableProcessList) {
                //获取源表表名
            final String sourceTable = tableProcess.getSourceTable();
            //获取操作类型 insert update delete
            final String operateType = tableProcess.getOperateType();
            //输出类型  hbase/kafka
            final String sinkType = tableProcess.getSingType();
            //输出的目标表名或主题名
            final String sinkTable = tableProcess.getSinkTable();
            //输出字段
            final String sinkColumns = tableProcess.getSinkColumns();
            //表主键
            final String sinkPK = tableProcess.getSinkPK();
            //建表扩展语句
            final String sinkExtend = tableProcess.getSinkExtend();
            //保存到map
            String key = sourceTable+":"+operateType;
            //将从配置表查询到的配置信息，保存到内存的map集合中
            tableProcessMap.put(key,tableProcess);
            //如果配置项中的sink表是hbase，需要检查表的状况
            if(TableProcess.SINK_TYPE_HBASE.equals(sinkType) && "insert".equals(operateType)){
                final boolean noExist = existsTables.add(sourceTable);
                //如果在内存set集合中不存在这个表，那么在phoenix中创建这张表
                if(noExist){
                    //检查phoenix中是否存在这张表，可能已经存在，但是缓存因为一些原因被清空
                    checkTable(sinkTable,sinkColumns,sinkPK,sinkExtend);
                }
            }

        }
        if(tableProcessMap == null || tableProcessMap.size() == 0 ){
            //说明配置表没有数据
            throw new RuntimeException("没有从配置表中读取到信息");
        }


    }

    private void checkTable(String tableName, String fields, String pk, String ext) {
        //如果配置表没有配置主键pk或者主键扩展,给一个默认
        if(pk == null){
            pk = "id";
        }
        if(ext == null ){
            ext = "";
        }
        //拼接建表语句
        StringBuilder createSql = new StringBuilder("create table if not exists " +
                GmallConfig.HBASE_SCHEMA + "." +tableName + "(");
        //对建表字段进行切分
        final String[] fieldsArr = fields.split(",");
        for(int i = 0; i < fieldsArr.length;i++){
            final String field = fieldsArr[i];
            //判断当前字段是否为主键字段
            if(pk.equals(field)){
                createSql.append(field).append(" varchar primary key ");
            }else{
                createSql.append("info.").append(field).append(" varchar ");
            }
            if(i < fieldsArr.length - 1 ){
                createSql.append(",");
            }

        }
        createSql.append(")").append(ext);

        System.out.println("创建phoenix 表的语句： " + createSql);

        //获取phoenix连接
         PreparedStatement ps = null;
        try{
            ps =  conn.prepareStatement(createSql.toString());
            ps.execute();
        }catch (SQLException e){
            e.printStackTrace();
        }finally {
            if(ps != null){
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

    }


}
