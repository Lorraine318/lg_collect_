package com.atguigu.gmall.realtime.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;
import redis.clients.jedis.Jedis;

import java.util.List;

/**
 * @author liugou
 * @date 2021/8/7 14:15
 *      用于查询维度表的工具类，底层调用的Phoenix工具类
 *      维度表查询肯定会根据具体条件来查找
 */
public class DimUtils {

    public static void main(String[] args) {
        DimUtils.getDimInfoNoChche("DIM_BASE_TRADEMARK",Tuple2.of("id","13"));
    }

        //select * from tableName where a= 1 and b=2
//        public static JSONObject getDimInfoNoChche(String tableName, Tuple2<String,String> ... cloNameAndValue){
//            //拼接查询条件
//            String whereSql = " where ";
//            for(int i = 0;  i < cloNameAndValue.length; i ++){
//                Tuple2<String,String> tuple2 = cloNameAndValue[i];
//                final String fieldName = tuple2.f0;
//                final String fieldValue = tuple2.f1;
//                if(i > 0 ){
//                    whereSql += " and ";
//                }
//                whereSql += fieldName + " = '" + fieldValue + "' ";
//            }
//            String sql = " select * from " +  tableName + whereSql;
//            System.out.println("查询维度的sql: " + sql);
//            final List<JSONObject> dimList = PhoenixUtils.queryList(sql, JSONObject.class);
//            //对于维度查询来讲，一般都是根据主键进行查询，不可能返回多条记录，只会有一条
//             JSONObject dimJsonObj = null;
//            if(dimList != null && dimList.size() > 0 ){
//                dimJsonObj  = dimList.get(0);
//            }else{
//                System.out.println("维度数据没有找到： " + sql);
//            }
//            return  dimJsonObj;
//        }

    public static JSONObject getDimInfoNoChche(String tableName,String id) {
       return  getDimInfoNoChche(tableName,id);
    }

    //根据 key 让 Redis 中的缓存失效
    public static void deleteCached( String tableName, String id){
        String key = "dim:" + tableName.toLowerCase() + ":" + id;
        try {
            Jedis jedis = RedisUtil.getJedis();
        // 通过 key 清除缓存
        jedis.del(key);
        jedis.close();
    } catch (Exception e) {
        System.out.println("缓存异常！");
        e.printStackTrace();
    }
}


        //优化：从phoenix中查询数据，加入旁路缓存，  先从缓存查询，如果缓存没有查到，再到phoenix查询，并将结果放到缓存中,每当缓存的数据发生变化时需要清除缓存，然后才能重新缓存。（如果判断可以根据之前在mysql中的配置来看是否是更新操作）
        //类型string, hash, list, zset , set
        //value: 通过phoenixUtil到维度表中查询数据，取初第一条并将其转换为json字符串
    public static JSONObject getDimInfoNoChche(String tableName, Tuple2<String,String> ... cloNameAndValue){
        //拼接查询条件
        String whereSql = " where ";
        String rediskey = "dim:" + tableName.toLowerCase() + ": ";
        for(int i = 0;  i < cloNameAndValue.length; i ++){
            Tuple2<String,String> tuple2 = cloNameAndValue[i];
            final String fieldName = tuple2.f0;
            final String fieldValue = tuple2.f1;
            if(i > 0 ){
                whereSql += " and ";
                rediskey += "_";
            }
            whereSql += fieldName + " = '" + fieldValue + "' ";
            rediskey += fieldValue;
        }
        //从redis中获取数据
        Jedis jedis = null;
        String disJsonStr = null; //维度数据的json字符串行式
        JSONObject dimJsonObj = null;
        try{
                jedis = RedisUtil.getJedis();
                //根据key到redis查询
            disJsonStr = jedis.get(rediskey);

        }catch (Exception e){
                e.printStackTrace();
        }

        //判断是否从redis查询到数据
        if(disJsonStr != null && disJsonStr.length() > 0 ){
            dimJsonObj = JSON.parseObject(disJsonStr);
        }else{
            //如果在redis中没有查到数据，到phoenix
            String sql = " select * from " +  tableName + whereSql;
            System.out.println("查询维度的sql: " + sql);
            final List<JSONObject> dimList = PhoenixUtils.queryList(sql, JSONObject.class);
            //对于维度查询来讲，一般都是根据主键进行查询，不可能返回多条记录，只会有一条

            if(dimList != null && dimList.size() > 0 ){
                dimJsonObj  = dimList.get(0);
                //将查询出来的数据放到redis
                if (jedis != null) {
                    jedis.setex(rediskey,3600*24,dimJsonObj.toJSONString());
                }
            }else{
                System.out.println("维度数据没有找到： " + sql);
            }
        }

        //关闭redis
        if(jedis != null ){
            jedis.close();
        }

        return  dimJsonObj;
    }




}
