package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.Index.IndexHolder;
import com.alibabacloud.polar_race.engine.common.Index.KeyCache;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.io.IoOperation;
import com.alibabacloud.polar_race.engine.common.pc.Producer;
import com.alibabacloud.polar_race.engine.common.util.ChannleHolder;
import com.alibabacloud.polar_race.engine.common.util.IoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class EngineRace extends AbstractEngine {

    private static final Logger LOGGER = LoggerFactory.getLogger(EngineRace.class);
    private CountDownLatch latch = new CountDownLatch(64);
    CyclicBarrier barrier1 = new CyclicBarrier(64);
    Producer producer;
    int sum;
    private KeyCache keyCache;
    private IndexHolder indexHolder;
    private String paths;


    @Override
    public void open(String path) throws EngineException {
        File file = new File(path);
        paths = path;
        if (!file.exists()) {
            file.mkdirs();
            IoUtils.createChannle(path);
        } else {
            IoUtils.createChannle(path);
            try {
                sum = getRecordSize();
                //正确性检测阶段
                if (sum < 64000000) {
                    producer = new Producer(180000);
                    sum = sum / 2;
                    LOGGER.debug("total size is {}", sum);
                    indexHolder = IndexHolder.getInstance(101011);
                    keyCache = KeyCache.getInstance(100000);
                    load(indexHolder, keyCache);
                } else {
                    FileChannel signChannel = ChannleHolder.getSignChannel();
                    if (signChannel.size() == 0) {
                        ByteBuffer buffer = ByteBuffer.allocate(4);
                        buffer.putInt(1);
                        buffer.flip();
                        signChannel.write(buffer);
                        indexHolder = IndexHolder.getInstance(253602);
                        load(indexHolder, null);
                    } else {
                        LOGGER.debug("total size is {}", sum);
                        producer = new Producer(252000);
                        keyCache = KeyCache.getInstance(252000);
                        load(null, keyCache);
//                        new Thread(new Runnable() {
//                            @Override
//                            public void run() {
//                                try {
//                                    Thread.sleep(600000);
//                                    LOGGER.debug(" start to exit ....");
//                                } catch (Exception e) {
//                                    e.printStackTrace();
//                                }
//                                System.exit(-1);
//                            }
//                        }).start();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            //加载map缓存
        }

    }

    @Override
    public void write(byte[] key, byte[] value) throws EngineException {
        int index = IoUtils.keyHash(key);
        synchronized (ChannleHolder.getKeyChannel(index)) {
            //写value文件
            int position = IoOperation.writeValueFile(value, index);
            //写key文件
            IoOperation.writeKeyFile(key, position, index);
        }
    }

    @Override
    public byte[] read(byte[] key) throws EngineException {
        long hash = IoUtils.byteArrayToLong(key);
        int index = IoUtils.keyHash(key);
        //如果内存索引中有，直接读position
        int position = indexHolder.get(index, hash);
        FileChannel fileChannel = ChannleHolder.getValueChannel(index);
        return IoOperation.positionBasedreadFile(fileChannel, position);
    }

    @Override
    public void range(byte[] lower, byte[] upper, AbstractVisitor visitor) throws EngineException {
        //LOGGER.debug("range");
        int threadId = (int) Thread.currentThread().getId() & 0x3f;
        if (threadId == 0) {
            producer.produce(keyCache, barrier1, visitor, paths);
        } else {
            try {
                int temp = 0;
                //LOGGER.debug("sum is",sum);
                while (temp < sum) {
                    barrier1.await();
                    for (int j = 0; j < producer.bufferSize; j++) {
                        visitor.visit(producer.keys[j], producer.values[producer.positions[j]]);
                    }
                    temp += producer.bufferSize;
                    //LOGGER.debug("threadId {} Consumer size {} ", threadId, temp);
                    barrier1.await();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }


    @Override
    public void close() {

    }


    private void load(IndexHolder indexHolder, KeyCache keyCache) {
        ExecutorService executorService = Executors.newFixedThreadPool(64);
        for (int i = 0; i < 64; i++) {
            final int a = i;
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    IoOperation.loadCache(a, indexHolder, keyCache);
                    latch.countDown();
                }
            });
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            executorService.shutdown();
        }
    }

    private int getRecordSize() {
        int sum = 0;
        try {
            for (int i = 0; i < 256; i++) {
                sum += ChannleHolder.getKeyChannel(i).size();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return sum / 12;
    }


}
