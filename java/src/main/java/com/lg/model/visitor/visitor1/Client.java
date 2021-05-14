package com.lg.model.visitor.visitor1;

import com.lg.model.visitor.visitor1.ConcreteVisitor.BusinessReport;
import com.lg.model.visitor.visitor1.visitor.CEOVisitor;
import com.lg.model.visitor.visitor1.visitor.CTOVisitor;

/**
 * @author liugou.
 * @date 2021/5/14 10:14
 */
public class Client {

    public static void main(String[] args) {
        // 构建报表
        BusinessReport report = new BusinessReport();
        System.out.println("=========== CEO看报表 ===========");
        report.showReport(new CEOVisitor());
        System.out.println("=========== CTO看报表 ===========");
        report.showReport(new CTOVisitor());
    }
}
