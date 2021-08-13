package com.atguigu.gmall.realtime.utils;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author liugou.
 * @date 2021/8/11 14:59
 *
 *      创建单例的线程池对象工具类
 */
public class ThreadPoolUtil {

    private static ThreadPoolExecutor pool;

    public static ThreadPoolExecutor getInstance(){

        if(pool == null ){
            synchronized (ThreadPoolUtil.class){
                if(pool == null ){
                    pool =   new ThreadPoolExecutor(4,20,300,
                            TimeUnit.SECONDS,new LinkedBlockingDeque<Runnable>(Integer.MAX_VALUE));
                }
            }
        }
        return pool;
    }

}
