package com.unloadhome.extendhash;

import org.apache.commons.codec.digest.MurmurHash3;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class ExtendHash {

    private static final int SEED=0;

    public static byte[] hash(byte[] input){
        long[] hashRes= MurmurHash3.hash128x64(input, 0, input.length, SEED);
        return asBytes(hashRes);
    }

    public static Integer hash(byte[] input,int num){
        return MurmurHash3.hash32x86(input, 0, input.length, SEED);
    }

    public static byte[] mergeSum2Key(byte[] dataSum, byte[] key) {
        int hashOffset = key.length - 16;
        System.arraycopy(dataSum, 0, key, hashOffset, 16);
        return key;
    }

    public static void hashRes2Key(byte[] data, byte[] key) {
        int hashOffset = key.length - 16;
        byte[] dataHash = hash(data);
        System.arraycopy(dataHash, 0, key, hashOffset, 16);
    }

    public static byte[] asBytes(long[] hash) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(8 * hash.length);
        byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        int len = hash.length;
        for (long item : hash) {
            byteBuffer.putLong(item);
        }
        return byteBuffer.array();
    }
}
