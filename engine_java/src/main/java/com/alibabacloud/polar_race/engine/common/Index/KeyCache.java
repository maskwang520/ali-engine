package com.alibabacloud.polar_race.engine.common.Index;

public class KeyCache {

    public  byte[][][] keyArrayCache;
    public  int[][] positionCache;
    public  int[] lenRec = new int[256];
    private static volatile  KeyCache instance;

    public byte[] getKey(int fileIndex, int index) {
        return keyArrayCache[fileIndex][index];
    }

    public void setKey(int fileIndex, int index, byte[] key) {
        keyArrayCache[fileIndex][index] = key;
    }

    private KeyCache(int initCapacity) {
        //ave:[256][251100][8]
        keyArrayCache = new byte[256][initCapacity][8];
        positionCache = new int[256][initCapacity];
    }

    public int getPosition(int fileIndex, int index) {
        return positionCache[fileIndex][index];
    }

    public void setPositionCache(int fileIndex, int index, int position) {
        positionCache[fileIndex][index] = position;
    }

    public int getLenRec(int fileIndex) {
        return lenRec[fileIndex];
    }

    public void setLenRec(int fileIndex, int value) {
        lenRec[fileIndex] = value;
    }

    public int compare(byte[] key1, byte[] key2) {
        int a,b;
        for (int i = 0; i < 8; i++) {
            a=key1[i]&0xff;
            b=key2[i]&0xff;
            if (a != b) {
                return a-b;
            }
        }
        return 0;
    }

    public void quicksort(int fileIndex, int left, int right) {
        if(left>=right){
            return;
        }
        int dp;
        dp = partition(fileIndex, left, right);
        quicksort(fileIndex, left, dp - 1);
        quicksort(fileIndex, dp + 1, right);

    }

    private int partition(int fileIndex, int left, int right) {
        byte[] pivot = keyArrayCache[fileIndex][left];
        int prePosition = positionCache[fileIndex][left];
        while (left < right) {
            while (left < right && compare(keyArrayCache[fileIndex][right], pivot) > 0)
                right--;
            if (left < right) {
                positionCache[fileIndex][left] = positionCache[fileIndex][right];
                keyArrayCache[fileIndex][left] = keyArrayCache[fileIndex][right];
                left++;
            }
            while (left < right && compare(keyArrayCache[fileIndex][left], pivot) < 0)
                left++;
            if (left < right) {
                positionCache[fileIndex][right] = positionCache[fileIndex][left];
                keyArrayCache[fileIndex][right] = keyArrayCache[fileIndex][left];
                right--;
            }
        }
        keyArrayCache[fileIndex][left] = pivot;
        positionCache[fileIndex][left] = prePosition;
        return left;
    }

    public static KeyCache getInstance(int initCapacity) {
        if (instance == null) {
            synchronized (KeyCache.class) {
                if (instance == null) {
                    instance = new KeyCache(initCapacity);
                }
            }
        }
        return instance;
    }
}
