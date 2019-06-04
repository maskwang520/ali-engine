package com.alibabacloud.polar_race.engine.common.io;

import com.alibabacloud.polar_race.engine.common.Index.IndexHolder;
import com.alibabacloud.polar_race.engine.common.Index.KeyCache;
import com.alibabacloud.polar_race.engine.common.util.ChannleHolder;
import com.alibabacloud.polar_race.engine.common.util.IoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Maskwang on 2018/11/7.
 */
public class IoOperation {

    private static final Logger LOGGER = LoggerFactory.getLogger(IoOperation.class);

    private static ThreadLocal<ByteBuffer> headpBuffers = new ThreadLocal<ByteBuffer>() {
        @Override
        protected ByteBuffer initialValue() {
            return ByteBuffer.allocate(4096);
        }
    };


    //    private static ThreadLocal<ByteBuffer> keyBuffers = new ThreadLocal<ByteBuffer>() {
//        @Override
//        protected ByteBuffer initialValue() {
//            return ByteBuffer.allocate(16);
//        }
//    };
    private static AtomicInteger[] longAdders = new AtomicInteger[256];

    static {
        for (int i = 0; i < 256; i++) {
            longAdders[i] = new AtomicInteger();
        }
    }


    //根据在文件中的positon读取value
    public static byte[] positionBasedreadFile(FileChannel fileChannel, int postion) {
        //FileChannel fileChannel = ChannleHolder.getValueChannel(index);
        ByteBuffer byteBuffer = headpBuffers.get();
        try {
            fileChannel.read(byteBuffer, postion * 4096L);
        } catch (Exception e) {
            e.printStackTrace();
        }
        byteBuffer.clear();
        return byteBuffer.array();
    }

    /**
     * 向指定文件写入value,并返回在文件中的位置
     *
     * @param value
     * @return
     */
    public static int writeValueFile(byte[] value, int index) {
        FileChannel fileChannel = ChannleHolder.getValueChannel(index);
        ByteBuffer buffer = headpBuffers.get();
        buffer.put(value);
        buffer.flip();
        int start = longAdders[index].getAndIncrement();
        long position = start * 4096L;
        try {
            fileChannel.write(buffer, position);
        } catch (Exception e) {
            e.printStackTrace();
        }
        buffer.clear();
        return start;
    }

    //写入key值,position
    public static void writeKeyFile(byte[] key, int pathPosition, int index) {
        FileChannel fileChannel = ChannleHolder.getKeyChannel(index);
        ByteBuffer byteBuffer = headpBuffers.get();
        //ByteBuffer byteBuffer = ByteBuffer.allocate(16);
        byteBuffer.put(key);
        byteBuffer.putInt(pathPosition);
        byteBuffer.flip();
        try {
            fileChannel.write(byteBuffer);
        } catch (Exception e) {
            e.printStackTrace();
        }
        byteBuffer.clear();
    }

    //load cache 加载缓存
    public static void loadMapCache(int start, int end,IndexHolder indexHolder) {
        try {
            ByteBuffer keyBuffer = ByteBuffer.allocate(8);
            ByteBuffer positionBuffer = ByteBuffer.allocate(4);
            for (int i = start; i <= end; i++) {
                // LOGGER.debug("load file{}..",i);
                FileChannel fileChannel = ChannleHolder.getKeyChannel(i);
                int position = 0;
                int sum = (int)fileChannel.size();
                //LOGGER.debug("file{} sum {}",i,sum);
                //MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, sum);
                while (position < sum) {
                    fileChannel.read(keyBuffer);
                    fileChannel.read(positionBuffer);
                    position += 12;
                    keyBuffer.flip();
                    long keyLongVal = IoUtils.byteArrayToLong(keyBuffer.array());
                    positionBuffer.flip();
                    int tempPosition = positionBuffer.getInt();
                    indexHolder.putIndex(i, keyLongVal, tempPosition);
                    keyBuffer.clear();
                    positionBuffer.clear();
                }
                //LOGGER.debug("map {} is {}",i,index);

            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void loadArrayCache(int start, int end,KeyCache keyCache,boolean parse) {
        try {
            ByteBuffer keyBuffer = ByteBuffer.allocate(8);
            ByteBuffer positionBuffer = ByteBuffer.allocate(4);
            for (int i = start; i <= end; i++) {
                FileChannel fileChannel = ChannleHolder.getKeyChannel(i);
                int position = 0;
                int index = 0;
                int sum = (int)fileChannel.size();
                fileChannel.position(0);
//                LOGGER.debug("file {} size is {}",i,sum);
                while (position < sum) {
                    fileChannel.read(keyBuffer);
                    fileChannel.read(positionBuffer);
                    position += 12;
                    keyBuffer.flip();
                    //byte[] tempKey = keyBuffer.array().clone();
                    byte[] tempKey = new byte[8];
                    System.arraycopy(keyBuffer.array(),0,tempKey,0,8);
                    positionBuffer.flip();
                    int tempPositon = positionBuffer.getInt();
                    int flag = -1;
                    if(parse) {
                        for (int j = index - 1; j >= 0; j--) {
                            if (keyCache.compare(keyCache.getKey(i, j), tempKey) == 0) {
                                //LOGGER.debug("same position is {}",j);
                                flag = j;
                                break;
                            }
                        }
                    }
                    if(flag==-1) {
                        keyCache.setKey(i, index, tempKey);
                        keyCache.setPositionCache(i,index,tempPositon);
                        index++;
                    }else{
                        keyCache.setPositionCache(i,flag,tempPositon);
                    }
                    keyBuffer.clear();
                    positionBuffer.clear();
                }
                //LOGGER.debug("array {} size is {}",i,index);
                keyCache.setLenRec(i,index);
                //long startTime = System.currentTimeMillis();
                keyCache.quicksort(i, 0, index - 1);
                //LOGGER.debug("file {} sort time is {} ms ",i,System.currentTimeMillis()-startTime);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //多线程加载
    public static void loadCache(int threadId,IndexHolder indexHolder,KeyCache keyCache) {
        //LOGGER.info("threadId is {}", threadId);
        int start = threadId * 4;
        if (indexHolder!=null&&keyCache==null) {
            loadMapCache(start, start + 3,indexHolder);
        } else {
            if(indexHolder==null&&keyCache!=null) {
                loadArrayCache(start, start + 3, keyCache,false);
            }else{
                loadMapCache(start, start + 3,indexHolder);
                loadArrayCache(start, start + 3, keyCache,true);
            }
        }

    }

    //顺序读
    public static void orderRead(FileChannel fileChannel,int start,int end,byte[][]values) {
        //FileChannel fileChannel = ChannleHolder.getValueChannel(index);
        ByteBuffer byteBuffer = headpBuffers.get();
        try {
            fileChannel.position(start*4096);
            for(int i=start;i<end;i++) {
                fileChannel.read(byteBuffer);
                System.arraycopy(byteBuffer.array(), 0, values[i], 0, 4096);
                byteBuffer.clear();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



}
