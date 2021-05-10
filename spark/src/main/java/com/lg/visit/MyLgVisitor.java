package com.lg.visit;

import com.lg.antlr4.LgBaseVisitor;
import com.lg.antlr4.LgParser;

/**
 * @author liugou.
 * @date 2021/5/10 15:08
 *      自定义词法符号 对应的操作逻辑
 */
public class MyLgVisitor extends LgBaseVisitor<Object> {

    // TODO: 方法名是在g4语法文件中给出的
    @Override
    public Object visitAddOrSubtract(LgParser.AddOrSubtractContext ctx) {
        Object obj0 = ctx.expr(0).accept(this);
        Object obj1 = ctx.expr(1).accept(this);
        //如果是加
        if("+".equals(ctx.getChild(1).getText())){
            return (Float)obj0 + (Float)obj1;
        }else if ("-".equals(ctx.getChild(1).getText())){
            return (Float)obj0 - (Float)obj1;
        }
        return 0f;
    }

    // TODO:  转换小数，方法名在g4语法文件中给出
    @Override
    public Object visitFloat(LgParser.FloatContext ctx) {
        return Float.parseFloat(ctx.getText());
    }
}
