package com.unloadhome.extendfilter;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Optional;

import com.unloadhome.extendhash.ExtendHash;

public class ExtendBloomFilterCell implements Serializable {
    private int count = 0;

    private byte[] keySum = null;

    private byte[] hashSum = new byte[16];

    private transient boolean pureCell = false;

    public ExtendBloomFilterCell() {
    }

    ExtendBloomFilterCell(int keySize) {
        this.hashSum = new byte[keySize];
    }

    synchronized void add(byte[] key) {
        ++this.count;
        this.xor(key, ExtendHash.hash(key));
    }

    void subtract(ExtendBloomFilterCell cell) {
        this.count -= cell.count;
        this.xor(cell.keySum, cell.hashSum);
        this.pureCell = Math.abs(this.count) == 1 && Arrays.equals(ExtendHash.hash(this.keySum), this.hashSum);
    }

    Optional<byte[]> recoverKeySum() {
        return this.pureCell ? Optional.of(this.keySum.clone()) : Optional.empty();
    }

    boolean recoverAble() {
        return this.pureCell;
    }

    ExtendBloomFilterCell copy() {
        ExtendBloomFilterCell copy = new ExtendBloomFilterCell(this.keySum.length);
        copy.count = this.count;
        copy.keySum = Arrays.copyOf(this.keySum, this.keySum.length);
        copy.hashSum = Arrays.copyOf(this.hashSum, 16);
        return copy;
    }

    boolean isEmpty() {
        if (this.count != 0) {
            return false;
        } else {
            byte[] var1 = this.keySum;
            int var2 = var1.length;

            int var3;
            byte bit;
            for (var3 = 0; var3 < var2; ++var3) {
                bit = var1[var3];
                if (bit != 0) {
                    return false;
                }
            }

            var1 = this.hashSum;
            var2 = var1.length;

            for (var3 = 0; var3 < var2; ++var3) {
                bit = var1[var3];
                if (bit != 0) {
                    return false;
                }
            }

            return true;
        }
    }

    private void xor(byte[] key, byte[] hashKey) {
        int i;
        for (i = 0; i < key.length; ++i) {
            this.keySum[i] ^= key[i];
        }

        for (i = 0; i < 16; ++i) {
            this.hashSum[i] ^= hashKey[i];
        }
    }

    public int getCount() {
        return this.count;
    }

    public void setCount(final int count) {
        this.count = count;
    }

    public byte[] getKeySum() {
        return this.keySum;
    }

    public void setKeySum(final byte[] keySum) {
        this.keySum = keySum;
    }

    public byte[] getHashSum() {
        return this.hashSum;
    }

    public void setHashSum(final byte[] hashSum) {
        this.hashSum = hashSum;
    }

}
