package com.lg.listener;

import com.lg.antlr4.LgBaseListener;
import com.lg.antlr4.LgParser;

/**
 * @author liugou.
 * @date 2021/5/10 15:54
 *      自定义词法符号对应的操作逻辑
 */
public class MyLgListener extends LgBaseListener  {

    // TODO:  加减对应的实现方法
    @Override
    public void exitAddOrSubtract(LgParser.AddOrSubtractContext ctx) {
        final String aCase = ctx.getChild(0).getText().toLowerCase();
        final String bCase = ctx.getChild(1).getText().toLowerCase();
        final String cCase = ctx.getChild(2).getText().toLowerCase();
        System.out.println(aCase + " 拼接 " + bCase + " 拼接 " + cCase);
    }


}
