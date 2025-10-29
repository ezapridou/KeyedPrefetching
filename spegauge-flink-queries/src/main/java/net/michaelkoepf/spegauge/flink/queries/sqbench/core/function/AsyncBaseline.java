package net.michaelkoepf.spegauge.flink.queries.sqbench.core.function;

import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.cache.Cache;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.cache.CacheAppend;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.cache.OperatorWithCacheHelper;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.sidehandling.SideHandle;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.sidehandling.StreamSide;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.internal.KeyAccessibleState;

import java.util.*;

public class AsyncBaseline<K, T> extends AsyncStateClient<K, T>{
    private final boolean cacheEnabled = true; // when false we use a dummy cache just to temporarily store fetched state

    public AsyncBaseline(Cache<K, T> cache, boolean windowing) {
        super(cache, windowing);
    }

    // ---------- internal I/O paths ----------

    /** Consumes a completed prefetch: deserializes bytes, inserts into the cache and applies window eviction if
     * necessary
     */
    @Override
    protected void processFetchResult(FetchResult<K, T> fetchResult, long windowSize, long windowSlide) {
        K compositeKey = cache.getKeyInterpreter().encode(fetchResult.key, fetchResult.side);

        try {
            List<T> deserializedState = fetchResult.state;

            long timestamp = inFlightRequests.get(compositeKey).latestNeededTs;
            List<Tuple2<K, List<T>>> evicted = cache.insertEntry(compositeKey, deserializedState, timestamp, false);
            writeEvictedElements(evicted);

            Long triggerTs = windowTriggers.remove(compositeKey);
            if (triggerTs != null) {
                OperatorWithCacheHelper.removeExpiredElementsCache((CacheAppend<K, ?>) cache, triggerTs, compositeKey,
                        windowSize, windowSlide);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            inFlightRequests.remove(compositeKey);
        }
    }

    // When cache is disabled, do not cache self upserts, but write straight to backend
    @Override
    public <V> boolean upsertSelf(StreamSide side, K compositeKey, V value, long ts, ListState<Tuple2<V, Long>> listState) {
        if (!cacheEnabled) {
            try { listState.add(Tuple2.of(value, ts)); } catch (Exception e) { throw new RuntimeException(e); }
            return false;
        }
        else {
            return super.upsertSelf(side, compositeKey, value, ts, listState);
        }
    }

    /** Delete the cached state for a key (only used when cache disabled). */
    public void dropCachedState(K compositeKey) {
        cache.remove(compositeKey);
    }

    public boolean isCacheEnabled() {
        return cacheEnabled;
    }

    private void writeEvictedElements(List<Tuple2<K, List<T>>> evicted) {
        if (evicted == null || evicted.isEmpty()) return;

        for (Tuple2<K, List<T>> e : evicted) {
            K compositeKey = e.f0;
            StreamSide streamSide = cache.getKeyInterpreter().sideOf(compositeKey);
            K key = cache.getKeyInterpreter().joinKey(compositeKey);
            fetchPool.submit(1, () -> {
                try {
                    KeyAccessibleState<K, List<T>> st =
                            requireHandle(streamSide).state;

                    st.updateAsync(key, e.f1);
                } catch (Throwable t) {
                    throw new RuntimeException(t);
                }
            });
        }
    }

}
