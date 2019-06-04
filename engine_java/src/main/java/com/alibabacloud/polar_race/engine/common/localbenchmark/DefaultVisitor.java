package com.alibabacloud.polar_race.engine.common.localbenchmark;

import com.alibabacloud.polar_race.engine.common.AbstractVisitor;
import com.alibabacloud.polar_race.engine.common.localbenchmark.util.DataUtils;
import com.alibabacloud.polar_race.engine.common.util.IoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

public class DefaultVisitor extends AbstractVisitor {

    private static Logger logger = LoggerFactory.getLogger(DefaultVisitor.class);

    public AtomicInteger visitCount = new AtomicInteger(0);


    public void visit(byte[] key, byte[] value) {
        byte[] _tmp = new byte[key.length];
        System.arraycopy(value, 0, _tmp, 0, _tmp.length);
        if (!Arrays.equals(key, _tmp)) {
            logger.error("range " + " 值不匹配:" + DataUtils.byteToString(_tmp)
                    + ", key:" + DataUtils.byteToString(key) + ", long:" + IoUtils.byteArrayToLong(key));
        } else {
//            System.out.println(visitCount.incrementAndGet());
            visitCount.incrementAndGet();
        }
    }
}
