package com.alibabacloud.polar_race.engine.common.util;

import java.nio.channels.FileChannel;

/**
 * Created by Maskwang on 2018/11/8.
 * 保存读写的channel
 */
public class ChannleHolder {

    private static FileChannel[] keyChannels = new FileChannel[256];
    private static FileChannel[] valueChannels = new FileChannel[256];
    private static FileChannel signChannel;

    public static void setKeyFileChannel(FileChannel keyFileChannel,int index){
        keyChannels[index]= keyFileChannel;
    }

    public static void setValueFileChannel(FileChannel valueFileChannel,int index){
       valueChannels[index] = valueFileChannel;
    }

    public static FileChannel getKeyChannel(int index) {
        return keyChannels[index];
    }

    public static FileChannel getValueChannel(int index) {
        return valueChannels[index];
    }

    public static FileChannel getSignChannel() {
        return signChannel;
    }

    public static void setSignChannel(FileChannel signChannel) {
        ChannleHolder.signChannel = signChannel;
    }

    //close channel
    public static void close(){
        try{

            for(int i=0;i<256;i++) {
                keyChannels[i].close();
                valueChannels[i].close();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }


}
