package net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.topN;

import net.michaelkoepf.spegauge.api.common.model.sqbench.EventWithTimestamp;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.AsyncBaseline;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.LatencyTracker;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.cache.CacheLRU;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.cache.OperatorWithCacheHelper;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.sidehandling.StreamSide;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.internal.KeyAccessibleState;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TopN_Async<K, T extends EventWithTimestamp>
        extends KeyedProcessFunction<K, T, List<T>> {
    protected final int n;

    protected transient ValueState<List<T>> topNElements;

    private KeyAccessibleState<K, List<T>> topNKeyAccessibleState;

    protected transient LatencyTracker latencyTracker;

    private EventTypeHelper<K, T> eventTypeHelper;

    // size of the cache in number of tuples
    private final int cacheSize = 4000000;

    private AsyncBaseline<K, T> asyncStateClient;

    private Map<K, PendingTuple<K, T>> pendingTuples = new HashMap<>();

    private StreamSide side = StreamSide.FIRST; // only one input, so always FIRST

    private long windowSize = Long.MAX_VALUE, windowSlide = 0; // no expiration of entries

    private boolean q18;

    static class PendingTuple<K, T> {
        public final K key;
        public final T tuple;
        public final long timestamp;

        public PendingTuple(K key, T tuple, long timestamp) {
            this.key = key;
            this.tuple = tuple;
            this.timestamp = timestamp;
        }
    }

    public TopN_Async(int n, boolean q18) {
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

        String stateName = "topNElements";
        topNElements = getRuntimeContext().getState(eventTypeHelper.getStateDescriptor(stateName));

        var cache = new CacheLRU<K, T>(cacheSize, true);

        topNKeyAccessibleState = OperatorWithCacheHelper.getKeyAccessibleValueStateList(topNElements, stateName);

        asyncStateClient = new AsyncBaseline<>(cache, false);
        asyncStateClient.wire(side, topNKeyAccessibleState);
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
        asyncStateClient.shutdown();
    }


    @Override
    public void processElement(T value, Context ctx, Collector<List<T>> out) {
        K key = ctx.getCurrentKey();
        long currentTime = ctx.timestamp();

        // check if any fetches are completed and drain them
        while (asyncStateClient.hasCompletedFetchesForPending()) {
            K drainedKey = asyncStateClient.drainOnePendingResult(windowSize, windowSlide);
            PendingTuple<K, T> pt = pendingTuples.remove(drainedKey);
            topNInCache(drainedKey, pt.tuple, pt.timestamp, out);
        }

        boolean hot = asyncStateClient.fetchKeyAsync(side, key, key, currentTime, true, windowSize, windowSlide);

        if (hot) {
            topNInCache(key, value, currentTime, out);
        }
        else {
            // remember this tuple for later processing
            pendingTuples.put(key, new PendingTuple<K, T>(key, value, currentTime));
        }

    }

    private void topNInCache(K key, T value, long currentTime, Collector<List<T>> out){
        List<T> stateForThisKey = asyncStateClient.readOrCreateEntryInCache(key, currentTime);

        int insertPos = 0;
        for (T element : stateForThisKey) {
            if (compareElements(value, element)){ // new record should be before current
                break;

            }
            insertPos++;
        }

        if (insertPos < n) {
            OperatorWithCacheHelper.insertTupleToCacheTopN(key, value, currentTime, asyncStateClient.cache,
                    topNKeyAccessibleState, insertPos, n);
            out.collect(new ArrayList<T>(stateForThisKey));
        }

        latencyTracker.updateLatency(1, value.processingTimeMilliSecondsStart, currentTime,
                getRuntimeContext().getTaskInfo().getIndexOfThisSubtask(), asyncStateClient.getCacheSize(),
                asyncStateClient.getMaxCacheSize());
    }

    protected boolean compareElements(T elem1, T elem2) {
        return eventTypeHelper.compareElements(elem1, elem2);
    }

}
