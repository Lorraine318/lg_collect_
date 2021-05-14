package com.lg.model.visitor.visitor1.ConcreteVisitor;

import com.lg.model.visitor.visitor1.element.EngineerStaff;
import com.lg.model.visitor.visitor1.element.ManagerStaff;
import com.lg.model.visitor.visitor1.element.Staff;
import com.lg.model.visitor.visitor1.visitor.IVisitor;

import java.util.LinkedList;
import java.util.List;

/**
 * @author liugou.
 * @date 2021/5/14 10:09
 */
public class BusinessReport {

    private List<Staff> mStaffs = new LinkedList<>();

    public BusinessReport() {
        mStaffs.add(new ManagerStaff("经理-A"));
        mStaffs.add(new EngineerStaff("工程师-A"));
        mStaffs.add(new EngineerStaff("工程师-B"));
        mStaffs.add(new EngineerStaff("工程师-C"));
        mStaffs.add(new ManagerStaff("经理-B"));
        mStaffs.add(new EngineerStaff("工程师-D"));
    }


    /**
     * 为访问者展示报表
     * @param visitor 公司高层，如CEO、CTO
     */
    public void showReport(IVisitor visitor) {
        for (Staff staff : mStaffs) {
            staff.accept(visitor);
        }
    }

}
