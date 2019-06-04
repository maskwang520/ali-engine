package com.alibabacloud.polar_race.engine.common.test;

/**
 * Created by Maskwang on 2018/11/7.
 */
public class Test {

//    static byte[][][] keyArrayCache;
//    static int[][] positionCache;

    public static void main(String[] args) throws Exception {
//
//        keyArrayCache = new byte[1][3][8];
//        byte[] key1 = new byte[]{30, 31, 33, 34, 35, 36, 39, 45};
//        byte[] key2 = new byte[]{30, 30, 33, 34, 35, 36, 39, 45};
//        byte[] key3 = new byte[]{30, 23, 33, 34, 35, 36, 39, 45};
//        keyArrayCache[0][0] =key1;
//        keyArrayCache[0][1] =key2;
//        keyArrayCache[0][2] =key3;
//
//        positionCache = new int[1][3];
//        positionCache[0][0] = 2;
//        positionCache[0][1] = 11;
//        positionCache[0][2] = 8;
//        quicksort(0,0,2);
//        for(int i=0;i<3;i++){
//            for(int j=0;j<8;j++) {
//                System.out.print(keyArrayCache[0][i][j]+"   ");
//            }
//            System.out.println("position:"+positionCache[0][i]);
//        }
//        AbstractEngine abstractEngine = new EngineRace();
//        abstractEngine.open("E:/alitest");
//        byte[][] keys = new byte[64][8];
//        for(int i=0;i<64;i++){
//            if(i<10) {
//                keys[i] = ("helloma" + i + "").getBytes();
//            }else{
//                keys[i] = ("hellom" + i + "").getBytes();
//            }
//        }
//        byte[] value = new byte[4096];
//        for (int i = 0; i < 4096; i++) {
//            value[i] = (byte) ((i % 26) + 'b');
//        }
//
//       ExecutorService executorService = Executors.newFixedThreadPool(64);
//        for(int i=0;i<64;i++) {
//            final int a = i;
//            executorService.execute(new Runnable() {
//                @Override
//                public void run() {
//                    try {
//                        abstractEngine.write(keys[a], value);
//                    }catch (Exception e){
//                        e.printStackTrace();
//                    }
//                }
//            });
//        }


        //       多线程去读
//        for(int i=0;i<64;i++) {
//            final int a = i;
//            executorService.execute(new Runnable() {
//                @Override
//                public void run() {
//                    try {
//                        byte[] result = abstractEngine.read(keys[a]);
//                        System.out.println(result);
//                      //  System.out.println("****"+a);
//                    }catch (Exception e){
//                        e.printStackTrace();
//                    }
//                }
//            });
//        }

//
//        for(int i=0;i<64;i++) {
//            final int a = i;
//            executorService.execute(new Runnable() {
//                @Override
//                public void run() {
//                    try {
//                        abstractEngine.range(new byte[]{1,2},new byte[]{1,2,3},new MyVisitor());
//                        //System.out.println(IndexHolder.containKey(IoUtils.keyHash(keys[a]),IoUtils.byteArrayToLong(keys[a])));
//                        //  System.out.println("****"+a);
//                    }catch (Exception e){
//                        e.printStackTrace();
//                    }
//                }
//            });
//        }


//        Thread.sleep(2000);
//        System.out.println(new String(abstractEngine.read("helloma1".getBytes())));


        //executorService.shutdown();
//        byte[] result1 = abstractEngine.read(keys[0]);
//        System.out.println(IndexHolder.containKey(IoUtils.keyHash(keys[0]),IoUtils.byteArrayToLong(keys[0])));
//        System.out.println(Arrays.equals(result1, abstractEngine.read(keys[1])));
//        System.out.println(new String(abstractEngine.read(keys[1])));
        // abstractEngine.close();


    }

//    public static void quicksort(int fileIndex, int left, int right) {
//        int dp;
//        if (left < right) {
//            dp = partition(fileIndex, left, right);
//            quicksort(fileIndex, left, dp - 1);
//            quicksort(fileIndex, dp + 1, right);
//        }
//    }
//
//    private static int partition(int fileIndex, int left, int right) {
//        byte[] pivot = keyArrayCache[fileIndex][left];
//        int prePosition = positionCache[fileIndex][left];
//        while (left < right) {
//            while (left < right && compare(keyArrayCache[fileIndex][right], pivot) > 0)
//                right--;
//            if (left < right) {
//                positionCache[fileIndex][left] = positionCache[fileIndex][right];
//                keyArrayCache[fileIndex][left++] = keyArrayCache[fileIndex][right];
//            }
//            while (left < right && compare(keyArrayCache[fileIndex][left], pivot) < 0)
//                left++;
//            if (left < right) {
//                positionCache[fileIndex][right] = positionCache[fileIndex][left];
//                keyArrayCache[fileIndex][right--] = keyArrayCache[fileIndex][left];
//            }
//        }
//        keyArrayCache[fileIndex][left] = pivot;
//        positionCache[fileIndex][left] = prePosition;
//        return left;
//    }
//
//    public static int compare(byte[] key1, byte[] key2) {
//        for (int i = 0; i < 8; i++) {
//            if (key1[i] != key2[i]) {
//                return key1[i] - key2[i];
//            }
//        }
//        return 0;
//    }


}
