package com.atguigu.gmall.realtime.common;

/**
 * @author liugou
 * @date 2021/5/9 9:43
 *      项目配置的常量类
 */
public class GmallConfig {

    //hbase 命名空间
    public static final String HBASE_SCHEMA = "GMALL0509_REALTIME";

    //phoenix 连接地址
    public static final String PHOENIX_SERVER = "jdbc:phoenix:hadoop202,hadoop203,hadoop204:2181";

}
