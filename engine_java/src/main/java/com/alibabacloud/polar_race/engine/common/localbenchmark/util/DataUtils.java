package com.alibabacloud.polar_race.engine.common.localbenchmark.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Author : shen xiao
 * Email  : 641827196@qq.com
 * Date   : 28/10/2018
 */


public class DataUtils {
    public static int byteArrayToInt(byte[] b) {
        return   b[3] & 0xFF |
                (b[2] & 0xFF) << 8 |
                (b[1] & 0xFF) << 16 |
                (b[0] & 0xFF) << 24;
    }

    public static byte[] intToByteArray(int a) {
        return new byte[] {
                (byte) ((a >> 24) & 0xFF),
                (byte) ((a >> 16) & 0xFF),
                (byte) ((a >> 8) & 0xFF),
                (byte) (a & 0xFF)
        };
    }

    public static short byteArrayToShort(byte[] b) {
        return  (short)((b[1] & 0xFF) |
                (b[0] & 0xFF) << 8);
    }

    public static byte[] shortToByteArray(short a) {
        return new byte[] {
                (byte) ((a >> 8) & 0xFF),
                (byte) (a & 0xFF)
        };
    }

    public static String byteToString(byte[] data){
        String ret = "";
        if(data == null){
            return "null";
        }

        StringBuffer buffer = new StringBuffer();

        // 把每一个byte 做一个与运算 0xff;
        for (byte b : data) {
            // 与运算
            int number = b & 0xff;// 加盐
            String str = Integer.toHexString(number);
            if (str.length() == 1) {
                buffer.append("0");
            }
            buffer.append(str);
        }

        // 标准的md5加密后的结果
        return buffer.toString();
    }

    public static String md5(byte[] data){
        MessageDigest md5 = null;
        try {
            md5 = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        byte[] hash = md5.digest(data);

        return byteToString(hash);
    }
}
