package com.lg.listener;

import com.lg.antlr4.LgLexer;
import com.lg.antlr4.LgParser;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

/**
 * @author liugou.
 * @date 2021/5/10 15:58
 *              监听者模式测试  词法语法解析
 */
public class Main {
    public static void main(String[] args) {
        String query = "3.1 +(6.3-4.51)";
        //todo 构建输入流，接收数据
        ANTLRInputStream inputStream = new ANTLRInputStream(query);
        //todo 构建词法分析器，用于处理输入流
        LgLexer lexer = new LgLexer(inputStream);
        //todo 新建词法符号缓冲区，用于存储词法分析器生成的词法符号
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        //todo 新建一个语法分析器，处理词法符号缓冲区内容
        LgParser parser = new LgParser(tokenStream);
        //todo 针对定义的规则，进行语法解析
        LgParser.ExprContext expr = parser.expr();
        //todo 构建自己的监听器
        MyLgListener listener = new MyLgListener();
        //todo 使用监听器初始化对语法树的遍历
        ParseTreeWalker.DEFAULT.walk(listener,expr);
    }
}
