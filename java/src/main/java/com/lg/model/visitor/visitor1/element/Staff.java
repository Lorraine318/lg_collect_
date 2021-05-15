package com.lg.model.visitor.visitor1.element;

import com.lg.model.visitor.visitor1.visitor.IVisitor;

import java.util.Random;

/**
 * @author liugou.
 * @date 2021/5/14 9:54
 *
 *      具体的元素类， 提供接受访问的具体实现
 */
public abstract  class Staff {

    public String name;
    public int kpi; //员工KPI

    public Staff(String name) {
        this.name = name;
        kpi = new Random().nextInt(10);
    }

    //核心方法,接受Visitor的访问
    public abstract void accept(IVisitor visitor);

}
