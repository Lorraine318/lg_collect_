package com.atguigu.gmall.realtime.app.func;


import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.OrderWide;
import com.atguigu.gmall.realtime.utils.DimUtils;
import com.atguigu.gmall.realtime.utils.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author liugou.
 * @date 2021/8/13 11:46
 * Desc:  自定义维度异步查询的函数
 *  *  模板方法设计模式
 *  *      在父类中只定义方法的声明，让整个流程跑通
 *  *      具体的实现延迟到子类中实现
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T,T> implements DimJoinFunction {

    //线程池对象的父接口
    private ExecutorService executorService = null;

    //维度表名
    private String tableName;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //初始化线程池对象
        executorService = ThreadPoolUtil.getInstance();
    }


    /**
     * 发送的异步请求方法
     * @param input  流中的事实数据
     * @param resultFuture  异步处理结束之后，返回结果
     */
    @Override
    public void asyncInvoke( T input,  ResultFuture<T> resultFuture) {
        //获取线程
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                try{
                    //发送异步请求

                    //从事实数据流中获取key
                    String key = getKey(input);

                    //根据维度主键到维度表中进行查询
                    long start = System.currentTimeMillis();
                    JSONObject dimInfoJsonObj = DimUtils.getDimInfo(tableName,key);
                    System.out.println("维度数据json格式: " + dimInfoJsonObj);
                    if(dimInfoJsonObj != null ){
                        //维度关联 ， 流中的事实数据和查询出来的维度数据进行关联，但是具体数据是不清楚的，抽象出来
                        join(input,dimInfoJsonObj);
                    }
                    System.out.println("维度关联后的对象: " + input);
                    long end = System.currentTimeMillis();
                    System.out.println("异步维度查询耗时： : " + (end - start) + "毫秒");
                    //将关联后的数据继续向下传递
                    resultFuture.complete(Arrays.asList(input));
                }catch (RuntimeException e ){
                        e.printStackTrace();
                        throw new RuntimeException("维度异步查询失败 : " + tableName);
                }

            }
        });
    }

    @Override
    public void timeout( T input,  ResultFuture<T> resultFuture) {
    }
}
