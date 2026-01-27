package net.michaelkoepf.spegauge.api.sut;

import java.io.Serializable;

public class BitSet implements Serializable {
    public boolean[] bitSet;

    public BitSet() {
        this(1);
    }

    public BitSet(int size) {
        bitSet = new boolean[size];
    }

    public void set(int index) {
        bitSet[index] = true;
    }

    public boolean get(int index) {
        return bitSet[index];
    }

    public void clear(int index) {
        bitSet[index] = false;
    }

    public int size() {
        return bitSet.length;
    }

    public BitSet and(BitSet other) {
        BitSet result = new BitSet(bitSet.length);
        for (int i = 0; i < bitSet.length; i++) {
            result.bitSet[i] = this.bitSet[i] & other.bitSet[i];
        }
        return result;
    }

    public BitSet or(BitSet other) {
        BitSet result = new BitSet(bitSet.length);
        for (int i = 0; i < bitSet.length; i++) {
            result.bitSet[i] = this.bitSet[i] | other.bitSet[i];
        }
        return result;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < bitSet.length; i++) {
            sb.append(bitSet[i] ? "1" : "0");
        }
        return sb.toString();
    }
}
