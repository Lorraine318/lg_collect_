package com.lg.model.visitor.visitor1.element;

import com.lg.model.visitor.visitor1.visitor.IVisitor;
import sun.reflect.generics.visitor.Visitor;

import java.util.Random;

/**
 * @author liugou.
 * @date 2021/5/14 10:02
 *    子类实现
 */
public class EngineerStaff extends Staff {

    public EngineerStaff(String name) {
        super(name);
    }

    @Override
    public void accept(IVisitor visitor) {
        visitor.visit(this);
    }

    // 工程师一年的代码数量
    public int getCodeLines() {
        return new Random().nextInt(10 * 10000);
    }
}
