package com.atguigu.gmall.realtime.utils;

import com.atguigu.gmall.realtime.bean.TableProcess;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author liugou
 * @date 2021/4/24 14:20
 *      从mysql数据中查询数据的工具类
 *      完成ORM,对象关系映射
 *       o: object对象 java对象
 *       r：relation关系  关系型数据库
 *       m:mapping映射    对象和关系型数据库表的记录的映射关系
 */
public class MySqlUtils {
        //sql:执行的查询语句
        //返回的数据类型
        public static <T> List<T> queryList(String sql,Class<T> clz,boolean underScoreToCamel){
                Connection conn = null;
            PreparedStatement ps =null;
            ResultSet rs = null;
               //注册驱动
            try {
                Class.forName("com.mysql.jdbc.Driver");
                //创建链接
                 conn = DriverManager.getConnection(
                        "jdbc:mysql://hadoop02:3306/gmall112021)realtime?characterEncoding=utf-8&useSSL=false",
                        "root",
                        "");
                //创建数据库连接对象
                  ps = conn.prepareStatement(sql);
                //执行sql语句
                  rs = ps.executeQuery();
                  //查询结果的元数据信息
                final ResultSetMetaData metaData = rs.getMetaData();
                final ArrayList<T> resultList = new ArrayList<>();
                //获取每一行数据
                while(rs.next()){
                    //将取出的数据封装为一个对象
                    final T obj = clz.newInstance();
                    //对查询的行数据的所有列进行遍历，获取每一列的名称
                    for(int i = 1; i < metaData.getColumnCount(); i++){
                        String columnName = metaData.getColumnName(i);
                        String propertyName = ""; //java类的属性名
                        if(underScoreToCamel){
                            //如果指定将下划线转换为驼峰命名法的值为true,需要将表中的列转换为类属性的驼峰命名法的形式
                            propertyName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL,columnName);
                        }
                        //调用apache  commons-beans中的工具类，给obj属性赋值
                        BeanUtils.setProperty(obj,propertyName,rs.getObject(i));
                    }
                    //将当前结果的一行数据封装的obj对象放到list集合中
                    resultList.add(obj);
                }

                //处理结果集
                return resultList;
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException("从mysql查询数据失败");
            }
            finally {
                //释放资源
                if(ps != null){
                    try {
                        ps.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
                if(rs != null){
                    try {
                        rs.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
                if(conn != null){
                    try {
                        conn.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }



        }


    public static void main(String[] args) {
        List<TableProcess> list = queryList("select * from table_process", TableProcess.class,true);
    }


}
