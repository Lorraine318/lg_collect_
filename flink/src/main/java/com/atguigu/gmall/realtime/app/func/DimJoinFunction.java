package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;

/**
 * @author liugou.
 * @date 2021/8/13 15:46
 *
 *  维度关联接口
 */
public interface DimJoinFunction<T>  {

    /**
     *需要提供一个获取key的方法，但是这个方法如何实现不知道，所以要抽象，该类也变为抽象类
     */
    //获取每个数据中的对应维度主键的key
      String getKey(T obj);
    //流中的事实数据和查询出来的维度数据进行关联
      void join(T obj, JSONObject dimInfoJsonObj);

}
