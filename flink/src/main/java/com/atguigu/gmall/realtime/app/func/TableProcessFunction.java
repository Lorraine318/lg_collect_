package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.utils.MySqlUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author liugou
 * @date 2021/4/24 15:31
 *      分流函数处理
 */
public class TableProcessFunction  extends ProcessFunction<JSONObject, JSONObject> {

    private OutputTag<JSONObject> outputTag;
    //
    private Map<String,TableProcess> tableProcessMap = new HashMap<>();

    public TableProcessFunction(OutputTag outputTag){
        this.outputTag = outputTag;
    }

    //在函数被调用的时候,执行一次。
    @Override
    public void open(Configuration parameters) throws Exception {

        refreshMeta();
    }
    //每条元素执行一次
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

            tableProcessMap.put(key,tableProcess);
        }

    }




}
