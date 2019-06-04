package com.alibabacloud.polar_race.engine.common.util;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

/**
 * Created by Maskwang on 2018/11/7.
 * IO工具类
 */
public class IoUtils {


    //把byteArray 转化成long
    public static long byteArrayToLong(byte[] key){
        long longKey=0;
        for(int i=0;i<8;++i){
            longKey<<=8;
            longKey |= (key[i]&0xff);
        }

        return longKey;
    }

    //根据路径创建文件
    public static File getFile(String path) {
        //创建文件
        File file = new File(path);
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return file;
    }



    public static void createChannle(String path){
        try {
            for(int i=0;i<256;i++){
                File keyFile = IoUtils.getFile(path + "/k"+i+".txt");
                FileChannel keyChannel = new RandomAccessFile(keyFile, "rw").getChannel();
                ChannleHolder.setKeyFileChannel(keyChannel,i);
                File valueFile = IoUtils.getFile(path + "/v"+i+".txt");
                FileChannel valueChannel = new RandomAccessFile(valueFile, "rw").getChannel();
                ChannleHolder.setValueFileChannel(valueChannel,i);
            }
            File keyFile = IoUtils.getFile(path + "/sign.txt");
            FileChannel channel = new RandomAccessFile(keyFile, "rw").getChannel();
            ChannleHolder.setSignChannel(channel);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

//    public static long getRealPosition(long value,long path){
//        path<<=35;
//        return value|path;
//    }
//
//    public static long[] getPathPosition(long value){
//        long []result = new long[2];
//        result[0] = (value>>35)&63;
//        result[1] = (value)&((1l<<35)-1);
//        return result;
//    }
    public static int hashValue(long value){
        return (int)(value&0x3f);
    }

    //映射到256个文件中
    public static int keyHash(byte[] key){
        return key[0]&0xff;
    }

}
