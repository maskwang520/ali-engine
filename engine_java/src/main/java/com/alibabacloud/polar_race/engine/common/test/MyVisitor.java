package com.alibabacloud.polar_race.engine.common.test;

import com.alibabacloud.polar_race.engine.common.AbstractVisitor;

public class MyVisitor extends AbstractVisitor {

    @Override
    public void visit(byte[] key, byte[] value) {
        System.out.println(new String(key));
    }
}
