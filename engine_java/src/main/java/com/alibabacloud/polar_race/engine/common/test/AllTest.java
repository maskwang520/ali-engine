package com.alibabacloud.polar_race.engine.common.test;

import com.alibabacloud.polar_race.engine.common.AbstractEngine;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.carrotsearch.hppc.LongArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * Created by Maskwang on 2018/11/10.
 */
public class AllTest {
    private static final Logger log = LoggerFactory.getLogger(AllTest.class);

    private static LongAdder writeCnt = new LongAdder();
    private static LongAdder readCnt = new LongAdder();

    private static Random random = new Random();

    private static LongArrayList dataList = null;

    /**
     * JVM parameter
     * <p>
     * -server -Xms2048m -Xmx2048m -XX:MaxDirectMemorySize=256m -XX:NewRatio=1 -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:-UseBiasedLocking
     * <p>
     * -server -XX:+UseG1GC -Xmx2048M -Xms2048M -XX:MaxDirectMemorySize=512m
     *
     http://polardbrace.oss-cn-shanghai.aliyuncs.com/log_186935.tar
     *
     * @param args
     * @throws EngineException
     */

    public static void main(String[] args) throws EngineException, InterruptedException {
        int TIMES = 10 * 10000;
        int threadnum = 64;

        dataList = new LongArrayList(TIMES * threadnum);
        for (int i = 0; i < threadnum * TIMES; i++) {
            dataList.add(random.nextLong());
            if (i % TIMES == 0) {
                log.debug(i + "");
            }
        }
        log.debug("randomSet (" + dataList.size() + ")");

        boolean initData = false;
        boolean testWrite = true;
        boolean testRead = true;

        String path = "src/tmp/t";
        delAll(initData, path);


        long allStart = System.nanoTime();
        if (testWrite) {
            long t1 = System.nanoTime();
            write(path, TIMES, threadnum);
            long t2 = System.nanoTime();
            log.warn("");
            log.warn("write time =>" + TimeUnit.NANOSECONDS.toMillis(t2 - t1));
            log.warn("");
        }
        long mid1 = System.nanoTime();

        if (testWrite && testRead) {
            log.warn("");
            log.warn("");
            log.warn("write & read wati 5s");
            log.warn("");
            log.warn("");
            TimeUnit.SECONDS.sleep(5);
        }

        long mid2 = System.nanoTime();
        if (testRead) {
            long t1 = System.nanoTime();
            read(path, TIMES, threadnum);
            long t2 = System.nanoTime();
            log.warn("");
            log.warn("read time =>" + TimeUnit.NANOSECONDS.toMillis(t2 - t1));
            log.warn("");
        }
        long allend = System.nanoTime();

        long custtimes = (mid1 - allStart) + (allend - mid2);
        log.warn("");
        log.warn("all times1 =>" + TimeUnit.NANOSECONDS.toMillis(custtimes));
        log.warn("all times2 =>" + TimeUnit.NANOSECONDS.toSeconds(custtimes));
        log.warn("");

        delAll(initData, path);

        System.exit(0);
    }

    private static void write(String path, int TIMES, int threadnum) throws EngineException, InterruptedException {
        AbstractEngine engineRace = new EngineRace();
        // open file
        engineRace.open(path);

        //write monitor
        if (TIMES * threadnum > 40 * 10000) {
            new Thread(() -> {
                while ((writeCnt.longValue() < TIMES * threadnum)) {
                    log.info("write monitor ==> ClientWriteCnts[" + writeCnt.longValue() + "] all cnts is [" + (TIMES * threadnum) + "]");
                    try {
                        TimeUnit.SECONDS.sleep(10);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                log.info("write monitor ==> ClientWriteCnts[" + writeCnt.longValue() + "] all cnts is [" + (TIMES * threadnum) + "]");
            }).start();
            TimeUnit.SECONDS.sleep(1);
        }


        CountDownLatch countDownLatch = new CountDownLatch(threadnum);
        ExecutorService writepools = Executors.newFixedThreadPool(threadnum);
        for (int i = 0; i < threadnum; i++) {
            writepools.submit(() -> {
                try {
                    for (int j = 0; j < TIMES; j++) {
                        long t = dataList.get(j);
                        engineRace.write(
                                l2b(t),
                                String.format("%4096d", t).getBytes());
                        writeCnt.increment();
                    }
                    countDownLatch.countDown();
                } catch (EngineException e) {
                    e.printStackTrace();
                }
            });
        }
        countDownLatch.await();
        log.debug("write over.");
        writepools.shutdownNow();
        writepools.awaitTermination(10,TimeUnit.SECONDS);
    }

    private static void read(String path, int TIMES, int threadnum) throws EngineException, InterruptedException {
        AbstractEngine engineRace = new EngineRace();
        // open file
        engineRace.open(path);

        //read monitor
        if (TIMES * threadnum > 40 * 10000) {
            new Thread(() -> {
                while ((readCnt.longValue() < TIMES * threadnum)) {
                    log.info("read monitor ==> ClientReadCnts[" + readCnt.longValue() + "] all cnts is [" + (TIMES * threadnum) + "]");
                    try {
                        TimeUnit.SECONDS.sleep(10);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                log.info("read monitor ==> ClientReadCnts[" + readCnt.longValue() + "] all cnts is [" + (TIMES * threadnum) + "]");
            }).start();
            TimeUnit.SECONDS.sleep(1);
        }

        //test read method
        CountDownLatch countDownLatch = new CountDownLatch(threadnum);
        ExecutorService readpools = Executors.newFixedThreadPool(threadnum);
        LongAdder exCnt = new LongAdder();
        for (int i = 0; i < threadnum; i++) {
            readpools.submit(() -> {
                for (int j = 0; j < TIMES; j++) {
                    try {
//                        long k = dataList.get(random.nextInt(TIMES));
                        long k = dataList.get(j);
                        byte[] ret = engineRace.read(l2b(k));
                        readCnt.increment();

                        if (k != Long.parseLong(new String(ret).trim())) {
                            log.error("curr key => " + k + "\t get val => " + new String(ret).trim());
                        }
                    } catch (EngineException ex) {
                        log.debug(ex.getLocalizedMessage()+"\t"+ex.retCode,ex);
                        exCnt.increment();
                        continue;
                    }
                }
                countDownLatch.countDown();
            });
        }
        countDownLatch.await();
        log.debug("read over.normal("+readCnt.longValue()+"),ex("+exCnt.longValue()+")");
        readpools.shutdownNow();
        readpools.awaitTermination(10,TimeUnit.SECONDS);

        // close event
        engineRace.close();
    }

    public static void delAll(boolean initData, String path) throws InterruptedException {
        if (initData) {
            for (File f : new File(path).listFiles()) {
                f.delete();
            }
            log.info("delete finish ,wait...");
            TimeUnit.SECONDS.sleep(5);
        }
    }

    public static byte[] l2b(long x) {
        byte[] buffer = new byte[8];
        for (int i = 0; i < 8; i++) {
            int offset = 64 - (i + 1) * 8;
            buffer[i] = (byte) ((x >> offset) & 0xff);
        }
        return buffer;
    }

    public static long b2l(byte[] b) {
        long values = 0;
        for (int i = 0; i < 8; i++) {
            values <<= 8;
            values |= (b[i] & 0xff);
        }
        return values;
    }
}
