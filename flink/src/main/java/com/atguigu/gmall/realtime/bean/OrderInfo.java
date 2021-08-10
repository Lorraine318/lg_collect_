package com.atguigu.gmall.realtime.bean;

import lombok.Data;

import java.math.BigDecimal;

/**
 * @author liugou
 * @date 2021/6/6 16:50
 *  订单数据
 */
@Data
public class OrderInfo {

    Long id;
    Long province_id;
    String order_statue;
    Long user_id;
    BigDecimal total_amount;
    BigDecimal activity_reduce_amount;
    BigDecimal coupon_reduce_amount;
    BigDecimal original_total_amount;
    BigDecimal feight_fee;
    String expire_time;
    String create_time;
    String operate_time;
    String create_date;
    String create_hour;
    Long create_ts;

}
