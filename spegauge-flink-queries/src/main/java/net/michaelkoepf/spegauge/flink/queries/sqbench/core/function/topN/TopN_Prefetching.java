package net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.topN;

import net.michaelkoepf.spegauge.api.common.model.sqbench.EventWithTimestamp;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.LatencyTracker;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.cache.CacheLRU;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.cache.OperatorWithCacheHelper;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.prefetching.Prefetcher;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.sidehandling.StreamSide;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.internal.KeyAccessibleState;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.operators.InputSelectable;
import org.apache.flink.streaming.api.operators.InputSelection;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TopN_Prefetching<K, T extends EventWithTimestamp>
        extends KeyedCoProcessFunction<K, Tuple2<K, Long>, T, List<T>> implements InputSelectable {
    protected final int n;

    protected transient ValueState<List<T>> topNElements;

    protected KeyAccessibleState<K, List<T>> topNKeyAccessibleState;

    protected transient LatencyTracker latencyTracker;

    protected EventTypeHelper<K, T> eventTypeHelper;

    // size of the cache in number of tuples
    protected final int cacheSize = 2000000;

    protected Prefetcher<K, T> prefetcher;
    //private long lastNormalTupleTs = 0, lastHintTs = 0;
    //private final Map<Long, Boolean> tupleAppearedFirstInHints = new HashMap<>();

    protected StreamSide side = StreamSide.FIRST; // only one input, so always FIRST

    private long windowSize = Long.MAX_VALUE, windowSlide = 0; // no expiration of entries

    private boolean q18;


    private Map<K, TopN_Async.PendingTuple<K, T>> pendingTuples = new HashMap<>();


    public TopN_Prefetching(int n, boolean q18) {
        this.n = n;
        this.q18 = q18;
    }

    @Override
    public void open(OpenContext parameters) throws Exception {
        if (q18) {
            eventTypeHelper = (EventTypeHelper<K, T>) new TopNNexmarkQ18EventHelper();
        } else {
            eventTypeHelper = (EventTypeHelper<K, T>) new TopNNexmarkQ19EventHelper();
        }

        latencyTracker = new LatencyTracker(200000);

        String stateName = "topNElements_";
        topNElements = getRuntimeContext().getState(eventTypeHelper.getStateDescriptor(stateName));

        var cache = new CacheLRU<K, T>(cacheSize, true);

        topNKeyAccessibleState = OperatorWithCacheHelper.getKeyAccessibleValueStateList(topNElements, stateName);

        prefetcher = new Prefetcher<K, T>(cache, false);
        prefetcher.wire(side, topNKeyAccessibleState);
        System.out.println("streamIndex,time,taskIndex,average,p50,p60,p70,p80,p90,p95,p99,p999,cacheSize,maxCacheSize");

        // uncomment lines bellow if you want to start the query with existing state
        /*int maxParallelism = 192;
        int operatorIndex = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        int parallelism = 48;
        int start = ((operatorIndex * maxParallelism + parallelism - 1) / parallelism);
        int end = ((operatorIndex + 1) * maxParallelism - 1) / parallelism;
        KeyGroupRange keyGroupRange = new KeyGroupRange(start, end);*/

        //eventTypeHelper.fillStaticTable(keyGroupRange, maxParallelism, topNKeyAccessibleState, n);
    }

    @Override
    public void close() {
        prefetcher.shutdown();
    }

    @Override
    public InputSelection nextSelection() {
        //return lastHintTs - lastNormalTupleTs > 100 ? InputSelection.SECOND : InputSelection.FIRST;
        return InputSelection.ALL;
    }

    @Override
    public void processElement1(Tuple2<K, Long> hint, Context ctx, Collector<List<T>> out) {
        K key = hint.f0;
        long timestampWhenStateNeeded = hint.f1;

        //lastHintTs = Math.max(lastHintTs, timestampWhenStateNeeded);

        if (prefetcher.cacheHasGottenFull) {
            prefetcher.fetchKeyAsync(side, key, key, timestampWhenStateNeeded, 1, false, windowSize, windowSlide);
        } else {
            prefetcher.readOrCreateEntryInCache(key, timestampWhenStateNeeded);
        }
    }

    @Override
    public void processElement2(T value, Context ctx, Collector<List<T>> out) {

        K key = ctx.getCurrentKey();
        long currentTime = ctx.timestamp();

        //lastNormalTupleTs = Math.max(lastNormalTupleTs, currentTime);

        // check if any fetches are completed and drain them
        while (prefetcher.hasCompletedFetchesForPending()) {
            K drainedKey = prefetcher.drainOnePendingResult(windowSize, windowSlide);
            //if (pendingTuples.containsKey(drainedKey)){
                TopN_Async.PendingTuple<K, T> pt = pendingTuples.remove(drainedKey);
                topNInCache(drainedKey, pt.tuple, pt.timestamp, out);
            //}
        }

        //boolean keyWasInCache = prefetcher.bringKeyToCacheSync(side, key, key, currentTime, windowSize, windowSlide);
        boolean keyWasInCache = prefetcher.fetchKeyAsync(side, key, key, currentTime, 2, true, windowSize, windowSlide); // async reads on pending

        // async reads on pending
        if (keyWasInCache) {
            topNInCache(key, value, currentTime, out);
        }
        else {
            // remember this tuple for later processing
            pendingTuples.put(key, new TopN_Async.PendingTuple<K, T>(key, value, currentTime));
        }

        prefetcher.setLastProcessedTs(currentTime);

        // we do not want to drain prefetching results if pending results where drained so that we do not delay the next
        // record too much
        if ( keyWasInCache && prefetcher.hasCompletedFetches()) { // !didDrainPending
            K drainedKey = prefetcher.drainOneResult(windowSize, windowSlide); // drain some prefetching results
            if (pendingTuples.containsKey(drainedKey)){
                TopN_Async.PendingTuple<K, T> pt = pendingTuples.remove(drainedKey);
                topNInCache(drainedKey, pt.tuple, pt.timestamp, out);
            }
        }
        prefetcher.drainWrites();

    }

    private void topNInCache(K key, T value, long currentTime, Collector<List<T>> out){
        List<T> stateForThisKey = prefetcher.readOrCreateEntryInCache(key, currentTime);

        int insertPos = 0;
        for (T element : stateForThisKey) {
            if (compareElements(value, element)){ // new record should be before current
                break;

            }
            insertPos++;
        }

        if (insertPos < n) {
            OperatorWithCacheHelper.insertTupleToCacheTopN(key, value, currentTime, prefetcher.cache,
                    topNKeyAccessibleState, insertPos, n);
            out.collect(stateForThisKey);
        }

        latencyTracker.updateLatency(1, value.processingTimeMilliSecondsStart, currentTime,
                getRuntimeContext().getTaskInfo().getIndexOfThisSubtask(), prefetcher.getCacheSize(),
                prefetcher.getMaxCacheSize(), prefetcher.getPrefetchingMisses(), prefetcher.getPrefetchingHits());
    }

    protected boolean compareElements(T elem1, T elem2) {
        return eventTypeHelper.compareElements(elem1, elem2);
    }

}
