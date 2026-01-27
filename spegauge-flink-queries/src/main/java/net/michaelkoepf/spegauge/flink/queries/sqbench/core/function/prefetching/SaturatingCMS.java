package net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.prefetching;

import java.io.Serializable;
import java.util.Random;

public class SaturatingCMS implements Serializable {
    private static final long serialVersionUID = 1L;
    private final int d; // Number of hash functions (rows)
    private final int w; // Width of the sketch (columns)
    private final int b; // Bits per counter
    private final int threshold; // Threshold T to classify as hot
    private final long maxValue; // Saturating limit (2^b - 1)

    private final long[][] counters;
    private final long[] hashSeeds;

    // Aging parameters
    private final int delta; // Aging interval (number of records)
    private int recordCounter = 0;

    public SaturatingCMS(){
        this(4, 10000, 8, 100, 1000);
    }

    public SaturatingCMS(int d, int w, int b, int threshold, int delta) {
        this.d = d;
        this.w = w;
        this.b = b;
        this.threshold = threshold;
        this.delta = delta;
        this.maxValue = (1L << b) - 1;
        this.counters = new long[d][w];

        // Initialize independent hash functions via seeds
        this.hashSeeds = new long[d];
        Random rand = new Random();
        for (int i = 0; i < d; i++) {
            hashSeeds[i] = rand.nextLong();
        }
    }

    /**
     * Updates the sketch with a new key and checks if it's hot.
     * Implements: C[i, hi(k)] = min{C[i, hi(k)] + 1, 2^b - 1}
     */
    public boolean update(Object key) {
        boolean isHot = true;
        int hashCode = key.hashCode();
        for (int i = 0; i < d; i++) {
            int bucket = getHash(hashCode, i);
            counters[i][bucket] = Math.min(counters[i][bucket] + 1, maxValue);
            if (counters[i][bucket] < threshold) {
                isHot = false; // If any counter is below T, it's not hot
            }
        }

        // Automatic Aging Pass
        recordCounter++;
        if (recordCounter >= delta) {
            age();
            recordCounter = 0;
        }
        return isHot;
    }

    /**
     * Returns true if the key is hot (all d counters >= T).
     */
    public boolean isHot(Object key) {
        int hashCode = key.hashCode();
        for (int i = 0; i < d; i++) {
            int bucket = getHash(hashCode, i);
            if (counters[i][bucket] < threshold) {
                return false; // If any counter is below T, it's not hot
            }
        }
        return true;
    }

    /**
     * Periodically ages the structure by dividing each counter by 2.
     * Implements: C[i, j] <- C[i, j] >> 1.
     */
    public void age() {
        for (int i = 0; i < d; i++) {
            for (int j = 0; j < w; j++) {
                counters[i][j] >>= 1;
            }
        }
    }

    private int getHash(int hashCode, int index) {
        // Simple seed-based mixing to simulate independent hash functions
        long hash = (hashCode ^ hashSeeds[index]) * 0x517cc1b727220a95L;
        int result = (int) ((hash ^ (hash >>> 32)) % w);
        return Math.abs(result);
    }
}
