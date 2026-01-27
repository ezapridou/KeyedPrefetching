package net.michaelkoepf.spegauge.generators.sqbench.datageneration;

import org.apache.commons.rng.UniformRandomProvider;

import java.util.stream.IntStream;
import java.util.stream.LongStream;

public final class EntityRecordGeneratorUtils {

    private EntityRecordGeneratorUtils() {
    }

    public static long[] getConsecutiveLongValues(long firstValue, long numValues) {
        return LongStream.range(firstValue, firstValue + numValues).toArray();
    }

    public static String[] generateRandomStringValues(UniformRandomProvider rng, int numValues, int length) {
        return IntStream.range(0, numValues).mapToObj(__ -> generateRandomString(rng, length)).toArray(String[]::new);
    }

    // copied from NEXMark Benchmark in Apache Beam
    public static String generateRandomString(UniformRandomProvider rng, int length) {
        if (length <= 0) {
            return null;
        }

        StringBuilder sb = new StringBuilder();
        int rnd = 0;
        int n = 0; // number of random characters left in rnd
        while (length-- > 0) {
            if (n == 0) {
                rnd = rng.nextInt();
                n = 6; // log_26(2^31)
            }
            sb.append((char) ('a' + rnd % 26));
            rnd /= 26;
            n--;
        }
        return sb.toString();
    }
}
