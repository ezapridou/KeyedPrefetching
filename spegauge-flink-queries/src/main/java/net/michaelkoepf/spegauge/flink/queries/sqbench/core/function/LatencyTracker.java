package net.michaelkoepf.spegauge.flink.queries.sqbench.core.function;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class LatencyTracker {
    int tuplesSincePrint = 0;
    long latency = 0;

    protected final List<Long> latencies = new ArrayList<>();

    int numTuplesBetweenPrints;

    long lastP999;

    public LatencyTracker(int numTuplesBetweenPrints) {
        this.numTuplesBetweenPrints = numTuplesBetweenPrints;
    }

    public void updateLatency(int streamId, long startTime, long currentTs, int subtaskIdx, int cacheSize, int maxCacheSize,
                              long misses, long hits ) {
        long endTime = System.currentTimeMillis();
        long latencyOfThisTuple = Math.max(endTime - startTime, 0);
        latency += latencyOfThisTuple;
        latencies.add(latencyOfThisTuple);


        if (tuplesSincePrint++ >= numTuplesBetweenPrints) {
            // time, avg, 99th in ns
            System.out.println(streamId + "," + currentTs + "," + subtaskIdx
                    +"," + latency / latencies.size() + "," + getPercentiles(latencies) + ","
                    + cacheSize + "," + maxCacheSize  + "," + misses + "," + hits);

            tuplesSincePrint = 0;
            latency = 0;
            latencies.clear();
        }
    }

    public void updateLatency(int streamId, long startTime, long currentTs, int subtaskIdx, int cacheSize, int maxCacheSize,
                              Map<Long, Boolean> tupleAppearedFirstInHints, long misses, long hits ) {
        long endTime = System.currentTimeMillis();
        long latencyOfThisTuple = Math.max(endTime - startTime, 0);
        latency += latencyOfThisTuple;
        latencies.add(latencyOfThisTuple);

        long firstHints = 0, firstTuple = 0;
        if (tupleAppearedFirstInHints != null) {
            for (Map.Entry<Long, Boolean> entry : tupleAppearedFirstInHints.entrySet()) {
                if (entry.getValue()) firstHints++;
                else firstTuple++;
            }
        }

        if (tuplesSincePrint++ >= numTuplesBetweenPrints) {
            // time, avg, 99th in ns
            System.out.println(streamId + "," + currentTs + "," + subtaskIdx
                    +"," + latency / latencies.size() + "," + getPercentiles(latencies) + ","
                    + cacheSize + "," + maxCacheSize + "," + firstHints + "(" + ((double)firstHints)/(firstTuple+firstHints) + ")," + firstTuple
                    + "," + misses + "," + hits);

            tuplesSincePrint = 0;
            latency = 0;
            latencies.clear();
        }
    }

    public void updateLatency(int streamId, long startTime, long currentTs, int subtaskIdx, int cacheSize, int maxCacheSize ) {
        long endTime = System.currentTimeMillis();
        long latencyOfThisTuple = Math.max(endTime - startTime, 0);
        latency += latencyOfThisTuple;
        latencies.add(latencyOfThisTuple);
        if (tuplesSincePrint++ >= numTuplesBetweenPrints) {
            // time, avg, 99th in ns
            System.out.println(streamId + "," + currentTs + "," + subtaskIdx
                    +"," + latency / latencies.size() + "," + getPercentiles(latencies) + ","
                    + cacheSize + "," + maxCacheSize);

            tuplesSincePrint = 0;
            latency = 0;
            latencies.clear();
        }
    }

    public long updateLatencyAndReturnP999(int streamId, long startTime, long currentTs, int subtaskIdx, int cacheSize, int maxCacheSize ) {
        updateLatency(streamId, startTime, currentTs, subtaskIdx, cacheSize, maxCacheSize);
        return lastP999;
    }

    public void updateLatency(int streamId, long startTime, long currentTs, int subtaskIdx, int cacheSize, int maxCacheSize, long totalElems) {
        long endTime = System.currentTimeMillis();
        long latencyOfThisTuple = Math.max(endTime - startTime, 0);
        latency += latencyOfThisTuple;
        latencies.add(latencyOfThisTuple);
        if (tuplesSincePrint++ >= numTuplesBetweenPrints) {
            // time, avg, 99th in ns
            System.out.println(streamId + "," + currentTs + "," + subtaskIdx
                    +"," + latency / latencies.size() + "," + getPercentiles(latencies) + ","
                    + cacheSize + "," + maxCacheSize + "," + totalElems);

            tuplesSincePrint = 0;
            latency = 0;
            latencies.clear();
        }
    }

    public void updateLatency(int streamId, long startTime, long currentTs, int subtaskIdx ) {
        long endTime = System.currentTimeMillis();
        long latencyOfThisTuple = Math.max(endTime - startTime, 0);
        latency += latencyOfThisTuple;
        latencies.add(latencyOfThisTuple);
        if (tuplesSincePrint++ >= numTuplesBetweenPrints) {
            // time, avg, 99th in ns
            System.out.println(streamId + "," + currentTs + "," + subtaskIdx
                    +"," + latency / latencies.size() + "," + getPercentiles(latencies));

            tuplesSincePrint = 0;
            latency = 0;
            latencies.clear();
        }
    }

    private String getPercentiles(List<Long> latencies) {
        if (latencies.isEmpty()) {
            return "";
        }
        List<Long> sorted = new ArrayList<>(latencies);
        Collections.sort(sorted);
        lastP999 = calculatePercentile(sorted, 99.9);
        return calculatePercentile(sorted, 50) + ","  +
                calculatePercentile(sorted, 60) + ","  +
                calculatePercentile(sorted, 70) + ","  +
                calculatePercentile(sorted, 80) + ","  +
                calculatePercentile(sorted, 90) + ","
                + calculatePercentile(sorted, 95) + "," +
                calculatePercentile(sorted, 99) + ","
                + lastP999;
    }

    private long calculatePercentile(List<Long> sortedLatencies, double percentile) {
        int index = (int) Math.ceil(percentile / 100.0 * sortedLatencies.size()) - 1;
        return sortedLatencies.get(Math.max(index, 0));
    }

}
