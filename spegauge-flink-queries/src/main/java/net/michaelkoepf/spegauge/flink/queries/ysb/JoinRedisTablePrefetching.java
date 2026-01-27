package net.michaelkoepf.spegauge.flink.queries.ysb;

import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.LatencyTracker;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.cache.CacheLRU;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.prefetching.Prefetcher;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.sidehandling.StreamSide;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.operators.InputSelectable;
import org.apache.flink.streaming.api.operators.InputSelection;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JoinRedisTablePrefetching
        extends KeyedCoProcessFunction<Long, Tuple2<Long, Long>, Tuple3<Long, String, Long>, Tuple3<Long, String, String>>
        implements InputSelectable {

    protected transient LatencyTracker latencyTracker;

    private final int cacheSize = 2_500_000; 

    private PrefetcherYSB prefetcher;

    private StreamSide side = StreamSide.FIRST; // only one input, so always FIRST

    private long windowSize = Long.MAX_VALUE, windowSlide = 0; // no expiration of entries

    private Map<Long, PendingTuple> pendingTuples = new HashMap<>();

    public static class PendingTuple {
        public final long key;
        public final Tuple3<Long, String, Long> tuple;
        public final long timestamp;

        public PendingTuple(long key, Tuple3<Long, String, Long> tuple, long timestamp) {
            this.key = key;
            this.tuple = tuple;
            this.timestamp = timestamp;
        }
    }


    @Override
    public void open(OpenContext parameters) throws Exception {
        latencyTracker = new LatencyTracker(200000);
        System.out.println("streamIndex,time,taskIndex,average,p50,p60,p70,p80,p90,p95,p99,p999");

        var cache = new CacheLRU<Long, Tuple3<Long, String, String>>(cacheSize, true);

        prefetcher = new PrefetcherYSB(cache, false);
    }

    @Override
    public void close() {
        prefetcher.shutdown();
    }

    @Override
    public InputSelection nextSelection() {
        return InputSelection.ALL;
    }

    @Override
    public void processElement1(Tuple2<Long, Long> hint, Context ctx, Collector<Tuple3<Long, String, String>> out) {
        long key = hint.f0;
        long timestampWhenStateNeeded = hint.f1;

        prefetcher.fetchKeyAsync(side, key, key, timestampWhenStateNeeded, 1, false, windowSize, windowSlide, "");
    }

    @Override
    public void processElement2(Tuple3<Long, String, Long> value, Context ctx, Collector<Tuple3<Long, String, String>> out) {
        String addId = value.f1;
        long addIdLong = value.f0;
        long currentTime = ctx.timestamp();

        while(prefetcher.hasCompletedFetchesForPending()){
            long drainedKey = prefetcher.drainOnePendingResult(windowSize, windowSlide);
            PendingTuple pt = pendingTuples.remove(drainedKey);
            enrichTuple(drainedKey, pt.tuple, pt.timestamp, out);
        }

        // ensure state is in cache now
        boolean keyWasInCache = prefetcher.fetchKeyAsync(side, addIdLong, addIdLong, currentTime, 2, true, windowSize, windowSlide, addId);

        if (keyWasInCache) {
            enrichTuple(addIdLong, value, currentTime, out);
        }
        else {
            // remember this tuple for later processing
            pendingTuples.put(addIdLong, new PendingTuple(addIdLong, value, currentTime));
        }

        prefetcher.setLastProcessedTs(currentTime);

        // we do not want to drain prefetching results if pending results where drained so that we do not delay the next
        // record too much
        if ( keyWasInCache && prefetcher.hasCompletedFetches()) { // !didDrainPending
            long drainedKey = prefetcher.drainOneResult(windowSize, windowSlide); // drain some prefetching results
            if (pendingTuples.containsKey(drainedKey)){
                PendingTuple pt = pendingTuples.remove(drainedKey);
                enrichTuple(drainedKey, pt.tuple, pt.timestamp, out);
            }
        }
        prefetcher.drainWrites();
    }

    private void enrichTuple(long addIDLong, Tuple3<Long, String, Long> value, long currentTime, Collector<Tuple3<Long, String, String>> out){
        List<Tuple3<Long, String, String>> stateForThisKeyList = prefetcher.readOrCreateEntryInCache(addIDLong, currentTime);
        if (stateForThisKeyList == null) {
            throw new RuntimeException("Problem. Unassigned value");
        }
        Tuple3<Long, String, String> enrichedTuple = stateForThisKeyList.get(0);
        if (enrichedTuple.f1.equals("")) {
            enrichedTuple.f1 = value.f1;
        }

        out.collect(enrichedTuple);

        latencyTracker.updateLatency(1, value.f2, currentTime,
                getRuntimeContext().getTaskInfo().getIndexOfThisSubtask(), prefetcher.getCacheSize(), prefetcher.getMaxCacheSize());

    }

}

