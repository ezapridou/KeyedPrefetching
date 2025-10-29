package net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.topN;

import net.michaelkoepf.spegauge.api.common.model.sqbench.EventWithTimestamp;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.cache.*;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.internal.KeyAccessibleState;
import org.apache.flink.util.Collector;

import java.util.List;

public class TopN_Cache<K, T extends EventWithTimestamp> extends TopN<K, T>{
    private KeyAccessibleState<K, List<T>> keyAccessibleState;
    private transient Cache<K, T> cache;
    private final int cacheSize = 2000000; // in number of tuples // Q19: (20gb/24 task slots/208 value size)
    private final int cacheType = 1; // 0 = TopKeys, 1 = LRU, 2 = Clock

    public TopN_Cache(int n, boolean q18) {
        super(n, q18);
    }

    @Override
    public void open(OpenContext parameters) throws Exception {
        super.open(parameters);

        if (cacheType == 1) {
            // LRU cache
            cache = new CacheLRU<K, T>(cacheSize, true);
        }
        else if (cacheType == 2) {
            // Clock cache
            cache = new CacheClock<K, T>(cacheSize, true);
        }

        keyAccessibleState = OperatorWithCacheHelper.getKeyAccessibleValueStateList(topNElements, "topNElements");

        // uncomment lines bellow if you want to start the query with existing state
        /*int maxParallelism = 192;
        int operatorIndex = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        int parallelism = 48;
        int start = ((operatorIndex * maxParallelism + parallelism - 1) / parallelism);
        int end = ((operatorIndex + 1) * maxParallelism - 1) / parallelism;
        KeyGroupRange keyGroupRange = new KeyGroupRange(start, end);*/

        //eventTypeHelper.fillStaticTable(keyGroupRange, maxParallelism, keyAccessibleState, n);
    }

    @Override
    public void processElement(T value, Context ctx,
                               Collector<List<T>> out) throws Exception {
        long startTime = value.processingTimeMilliSecondsStart;

        K key = ctx.getCurrentKey();
        long currentTime = ctx.timestamp();

        // if element in cache read from there, otherwise bring it to cache
        List<T> stateForThisKey = OperatorWithCacheHelper.getStateCache(key, key, keyAccessibleState, cache, currentTime);

        int insertPos = 0;
        for (T element : stateForThisKey) {
            if (compareElements(value, element)){ // new record should be before current
                break;
            }
            insertPos++;
        }
        if (insertPos < n) {
            OperatorWithCacheHelper.insertTupleToCacheTopN(key, value, currentTime, cache, keyAccessibleState, insertPos, n);
            out.collect(stateForThisKey);
        }

        latencyTracker.updateLatency(1, startTime, ctx.timestamp(),
                getRuntimeContext().getTaskInfo().getIndexOfThisSubtask(), cache.getCacheSize(), cache.getMaxCacheSize());

    }
}
