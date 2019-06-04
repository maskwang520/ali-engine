package com.alibabacloud.polar_race.engine.common.Index;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import com.carrotsearch.hppc.LongIntHashMap;

/**
 * Created by Maskwang on 2018/11/7.
 * 内存的key的索引 indexmap:<key转化成long,value在文件中的位置f>
 */
public class IndexHolder {

    //采用hppc试试
    private static LongIntHashMap[] objectLongHashMap = new LongIntHashMap[256];
    private static volatile IndexHolder instance;

//    static {
//        for (int i = 0; i < 256; i++) {
//            objectLongHashMap[i] = new LongIntHashMap(256000, 0.99);
//        }
//    }

    private IndexHolder(int initCapacity) {
        //ave:25600
        for (int i = 0; i < 256; i++) {
            objectLongHashMap[i] = new LongIntHashMap(initCapacity, 0.99);
        }
    }

    public void putIndex(int mapIndex, long key, int filePosition) {
        synchronized (objectLongHashMap[mapIndex]) {
            objectLongHashMap[mapIndex].put(key, filePosition);
        }
    }

    public int get(int mapIndex, long key) throws EngineException {
        int value = objectLongHashMap[mapIndex].get(key);
        if (value != 0) {
            return value;
        } else {
            if (containKey(mapIndex, key)) {
                return 0;
            } else {
                throw new EngineException(RetCodeEnum.NOT_FOUND, "not found the key");
            }
        }

    }

    public boolean containKey(int mapIndex, long key) {
        return objectLongHashMap[mapIndex].containsKey(key);
    }

    public static IndexHolder getInstance(int initCapacity) {
        if (instance == null) {
            synchronized (IndexHolder.class) {
                if (instance == null) {
                    instance = new IndexHolder(initCapacity);
                }
            }
        }
        return instance;
    }

}
