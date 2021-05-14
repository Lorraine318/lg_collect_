package com.lg.model.visitor.visitor1.visitor;

import com.lg.model.visitor.visitor1.element.EngineerStaff;
import com.lg.model.visitor.visitor1.element.ManagerStaff;

/**
 * @author liugou.
 * @date 2021/5/14 10:12
 */
public class CEOVisitor implements  IVisitor {
    @Override
    public void visit(EngineerStaff enginner) {
        System.out.println("工程师: " + enginner.name + ", KPI: " + enginner.kpi);
    }

    @Override
    public void visit(ManagerStaff manager) {
        System.out.println("经理: " + manager.name + ", KPI: " + manager.kpi +
                ", 新产品数量: " + manager.getProducts());
    }
}
