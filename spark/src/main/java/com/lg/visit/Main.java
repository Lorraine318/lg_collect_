package com.lg.visit;

import com.lg.antlr4.LgLexer;
import com.lg.antlr4.LgParser;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;

/**
 * @author liugou.
 * @date 2021/5/10 15:25
 *          访问者模式测试  词法语法解析
 */
public class Main {

    public static void main(String[] args) {
        String query = "6.3-4.51";
        //todo 构建输入流，接收数据
        ANTLRInputStream inputStream = new ANTLRInputStream(query);

        /**
         *词法分析器
         */
        //todo 构建词法分析器,用于处理输入流.
        LgLexer lgLexer = new LgLexer(inputStream);
        //todo 新建词法符号缓冲区，用于存储词法分析器生成的词法符号
        CommonTokenStream commonTokenStream = new CommonTokenStream(lgLexer);

        /**
         *语法分析器
         */
        // todo 新建一个语法分析器，处理词法符号缓冲区中的内容
        LgParser lgParser = new LgParser(commonTokenStream);


        // todo 创建访问者对象
        MyLgVisitor myLgVisitor = new MyLgVisitor();
        System.out.println(myLgVisitor.visit(lgParser.expr()));


    }
}
