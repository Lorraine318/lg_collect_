package com.lg.model.visitor.visitor1.element;

import com.lg.model.visitor.visitor1.visitor.IVisitor;
import sun.reflect.generics.visitor.Visitor;

import java.util.Random;

/**
 * @author liugou.
 * @date 2021/5/14 10:03
 *  子类实现
 */
public class ManagerStaff extends Staff {

    public ManagerStaff(String name) {
        super(name);
    }

    @Override
    public void accept(IVisitor visitor) {
        visitor.visit(this);
    }
    // 一年做的产品数量
    public int getProducts() {
        return new Random().nextInt(10);
    }
}
