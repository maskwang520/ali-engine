package com.alibabacloud.polar_race.engine.common.pc;

import com.alibabacloud.polar_race.engine.common.AbstractVisitor;
import com.alibabacloud.polar_race.engine.common.Index.KeyCache;
import com.alibabacloud.polar_race.engine.common.io.IoOperation;
import com.alibabacloud.polar_race.engine.common.util.IoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Producer {
    private static final Logger LOGGER = LoggerFactory.getLogger(Producer.class);
    CyclicBarrier cb = new CyclicBarrier(65);
    //public KvDouble[] buffer;
    public byte[][] keys;
    public byte[][] values;
    public int[] positions;
    public int bufferSize;


    public Producer(int intiCap) {
        values = new byte[intiCap][4096];
    }

    public void produce(KeyCache keyCache, CyclicBarrier barrier1, AbstractVisitor visitor, String path) {
        ExecutorService executorService = Executors.newFixedThreadPool(64);
        try {
            for (int count = 0; count < 256; count++) {
                int len = keyCache.getLenRec(count);
                if (len == 0) {
                    continue;
                }
                keys = keyCache.keyArrayCache[count];
                positions = keyCache.positionCache[count];
                bufferSize = len;
                final int a = count;
                LOGGER.debug("file {} size is {}", count, len);
                int segment = bufferSize / 63;
                for (int i = 0; i < 63; i++) {
                    final int b = i;
                    executorService.execute(new Runnable() {
                        @Override
                        public void run() {
                            multiProduce(a, path, b * segment, (b + 1) * segment);
                            try {
                                cb.await();
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    });
                }
                //处理分配不均衡的
                if(bufferSize%63!=0) {
                    executorService.execute(new Runnable() {
                        @Override
                        public void run() {
                            multiProduce(a, path, segment * 63, bufferSize);
                            try {
                                cb.await();
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    });
                }else{
                    executorService.execute(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                cb.await();
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    });
                }
                cb.await();
                barrier1.await();
                for (int k = 0; k < bufferSize; k++) {
                    visitor.visit(keys[k], values[positions[k]]);
                }
                barrier1.await();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            LOGGER.debug("shutdown");
            executorService.shutdown();
        }

    }

    public void multiProduce(int fileIndex, String path, int start, int end) {
        FileChannel valueChannel;
        File valueFile = IoUtils.getFile(path + "/v" + fileIndex + ".txt");
        try {
            valueChannel = new RandomAccessFile(valueFile, "rw").getChannel();
            IoOperation.orderRead(valueChannel, start, end, values);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
