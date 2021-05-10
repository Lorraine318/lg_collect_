package com.atguigu.gmall.realtime.bean;

import lombok.Data;

/**
 * @author liugou
 * @date 2021/4/24 14:13
 *      业务表的配置表
 */
@Data
public class TableProcess {
        //动态分流sink常量， 改为小写和脚本一致.
        public static final String SINK_TYPE_HBASE = "hbase";
        public static final String SINK_TYPE_KAFKA = "kafka";
        public static final String SINK_TYPE_CK = "clickhouse";

        //来源表
        String sourceTable;
        //操作类型 insert,update,delete
        String operateType;
        //输出表(主题)
        String sinkTable;
        //输出类型 hbase/kafka
        String singType;
        //输出字段
        String sinkColumns;
        //主键字段
        String sinkPK;
        //建表扩展
        String sinkExtend;


        public String getSourceTable() {
                return sourceTable;
        }

        public void setSourceTable(String sourceTable) {
                this.sourceTable = sourceTable;
        }

        public String getOperateType() {
                return operateType;
        }

        public void setOperateType(String operateType) {
                this.operateType = operateType;
        }

        public String getSinkTable() {
                return sinkTable;
        }

        public void setSinkTable(String sinkTable) {
                this.sinkTable = sinkTable;
        }

        public String getSingType() {
                return singType;
        }

        public void setSingType(String singType) {
                this.singType = singType;
        }

        public String getSinkColumns() {
                return sinkColumns;
        }

        public void setSinkColumns(String sinkColumns) {
                this.sinkColumns = sinkColumns;
        }

        public String getSinkPK() {
                return sinkPK;
        }

        public void setSinkPK(String sinkPK) {
                this.sinkPK = sinkPK;
        }

        public String getSinkExtend() {
                return sinkExtend;
        }

        public void setSinkExtend(String sinkExtend) {
                this.sinkExtend = sinkExtend;
        }
}
