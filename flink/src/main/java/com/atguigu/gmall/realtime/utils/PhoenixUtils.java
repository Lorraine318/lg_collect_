package com.atguigu.gmall.realtime.utils;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConfig;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author liugou
 * @date 2021/8/7 11:16
 *      查询phoenix的工具类
 */
public class PhoenixUtils {

    public static void main(String[] args) {
        queryList("select * from DIM_BASE_TRADEMARK", JSONObject.class);
    }

    private static Connection connection = null;

    public static void init(){
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
            connection.setSchema(GmallConfig.HBASE_SCHEMA);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static <T>List<T> queryList(String sql,Class<T> clazz){
            if(connection == null){
                init();
            }
        List<T> resultList = new ArrayList<>();
        PreparedStatement ps = null;
        ResultSet resultSet = null;
        try{
            //获取数据库操作对象
            ps =  connection.prepareStatement(sql);
            //执行sql语句
            resultSet = ps.executeQuery();
            //处理结果集
            final ResultSetMetaData metaData = resultSet.getMetaData();
            while(resultSet.next()){
                final T rowData = clazz.newInstance();
                for(int i = 1; i <= metaData.getColumnCount(); i++){
                    BeanUtils.setProperty(rowData,metaData.getColumnName(i),resultSet.getObject(i));
                }
                resultList.add(rowData);
            }

        }catch (Exception e){
            e.printStackTrace();
            throw new RuntimeException("从维度表查询数据失败");
        }finally {
            //释放资源
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if(ps != null){
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return resultList;
    }



}
