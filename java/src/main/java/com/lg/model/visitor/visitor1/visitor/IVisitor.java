package com.lg.model.visitor.visitor1.visitor;

import com.lg.model.visitor.visitor1.element.EngineerStaff;
import com.lg.model.visitor.visitor1.element.ManagerStaff;

/**
 * @author liugou.
 * @date 2021/5/13 11:53
 */
public interface IVisitor {

    //访问工程师类型
    void visit(EngineerStaff enginner);

    //访问经理类型
    void visit(ManagerStaff enginner);
}
