package com.lg.model.visitor.visitor1.visitor;

import com.lg.model.visitor.visitor1.element.EngineerStaff;
import com.lg.model.visitor.visitor1.element.ManagerStaff;

/**
 * @author liugou.
 * @date 2021/5/14 10:14
 */
public class CTOVisitor implements  IVisitor {
    @Override
    public void visit(EngineerStaff engineer) {
        System.out.println("工程师: " + engineer.name + ", 代码行数: " + engineer.getCodeLines());

    }

    @Override
    public void visit(ManagerStaff manager) {
        System.out.println("经理: " + manager.name + ", 产品数量: " + manager.getProducts());
    }
}
