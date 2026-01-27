package net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.prefetching;

import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.*;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.cache.Cache;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.cache.CacheAppend;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.cache.CacheLRU;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.cache.OperatorWithCacheHelper;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.sidehandling.SideHandle;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.sidehandling.StreamSide;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.internal.KeyAccessibleState;

import java.util.*;
import java.util.concurrent.*;

public class Prefetcher<K, T> extends AsyncStateClient<K, T> {
    // ---------- execution infra ----------
    /** Evicted entries awaiting async write-back: mergedKey -> write task. */
    protected transient final ConcurrentHashMap<K, WriteTask<K, T>> pendingWrites;

    // ---------- config ----------
    protected final int DRAIN_READS_MAX_OPS = 1, DRAIN_WRITES_MAX_OPS = 1;

    protected volatile long lastProcessedTs = 0L; // todo hacky way to make sure prefetching does not cause eviction of things we need

    public boolean cacheHasGottenFull = true;

    public Prefetcher(CacheLRU<K, T> cache, boolean windowing) {
        super(cache, windowing);
        pendingWrites = new ConcurrentHashMap<>();
    }

    public String stats() {
        return fetchResultQueue.size() + ",fetches queue, "+
                fetchResultQueuePending.size() + ",pending fetches, "+
                + inFlightRequests.size() + ",in-flight requests, "
                + pendingWrites.size() + ",pending writes, " + fetchPool.getQueue().size() +",queue";
    }

    // ---------- operator-facing API ----------

    public boolean bringKeyToCacheSync(StreamSide side, K compositeKey, K key, long ts, long windowSize, long windowSlide) {
        if (isHotOrCanBecomeWithoutBackendAccess(compositeKey, cache, side, ts)) {
            cache.touch(compositeKey, ts);
            prefetchingHits++;
            return true;
        }
        if (!cacheHasGottenFull) {
            readOrCreateEntryInCache(compositeKey, ts);
            prefetchingHits++;
            return true;
        }
        fetchStateToCacheSync(compositeKey, key, side, ts, windowSize, windowSlide);
        return false;
    }

    /** Drain up to maxReadDrains prefetch results and materialize them into the caches. */
    public boolean drainResults(long windowSize, long windowSlide) {
        int ops = 0;

        FetchResult<K, T> r;
        while (ops < DRAIN_READS_MAX_OPS && (r = fetchResultQueue.poll()) != null) {
            processFetchResult(r, windowSize, windowSlide);
            ops++;
        }
        return ops > 0;
    }

    /** Asynchronously flush up to maxWriteDrains buffered write-backs. */
    public void drainWrites() {
        if (pendingWrites.isEmpty()) return;
        fetchPool.submit(0, () -> {
            int drained = 0;
            for (Iterator<Map.Entry<K, WriteTask<K, T>>> it = pendingWrites.entrySet().iterator();
                 it.hasNext() && drained < DRAIN_WRITES_MAX_OPS; drained++) {

                Map.Entry<K, WriteTask<K, T>> evicted = it.next();
                WriteTask<K, T> wt = evicted.getValue();
                it.remove();

                try {
                    KeyAccessibleState<K, List<T>> st =
                            requireHandle(wt.side).state;

                    st.updateAsync(wt.key, wt.values);
                } catch (Throwable e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    // ---------- metrics ----------
    public void setLastProcessedTs(long ts) { this.lastProcessedTs = ts; }

    // ---------- internal I/O paths ----------

    /** Consumes a completed prefetch: deserializes bytes, inserts into the cache and applies window eviction if
     * necessary
     */
    @Override
    protected void processFetchResult(FetchResult<K, T> fetchResult, long windowSize, long windowSlide) {
        K compositeKey = cache.getKeyInterpreter().encode(fetchResult.key, fetchResult.side);
        //SideHandle<K, T> h = requireHandle(fetchResult.side);

        try {
            List<T> deserializedState = fetchResult.state;
                    /*(fetchResult.state == null || fetchResult.state.length == 0)
                            ? new ArrayList<>()
                            : h.state.deserializeRawBytes(fetchResult.state);*/

            long timestamp = inFlightRequests.get(compositeKey).latestNeededTs;
            List<Tuple2<K, List<T>>> evicted = ((CacheLRU<K, T>)cache).insertEntry(compositeKey, deserializedState, timestamp,
                    lastProcessedTs, false);
            bufferEvictedElementsForWrite(evicted);

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

    /** If there's a buffered write for (key,isStream1), serve it to the cache immediately. */
    private boolean tryServeFromWriteBuffer(K compositeKey, long ts, Cache<K, T> cache) {
        WriteTask<K, T> wt = pendingWrites.remove(compositeKey);
        if (wt == null) return false;

        // Put into cache (may evict other keys)
        List<Tuple2<K, List<T>>> ev = ((CacheLRU<K, T>)cache).insertEntry(
                compositeKey, wt.values, ts, lastProcessedTs, true);
        bufferEvictedElementsForWrite(ev); // enqueue any evicted entries for async write-back

        return true;
    }

    /** Buffers evicted cache contents so they can be written back to keyed state asynchronously.*/
    private void bufferEvictedElementsForWrite(List<Tuple2<K, List<T>>> evicted) {
        if (evicted == null || evicted.isEmpty()) return;

        if (!cacheHasGottenFull){
            clearHitsAndMisses();
            cacheHasGottenFull = true;
        }

        for (Tuple2<K, List<T>> e : evicted) {
            K key = cache.getKeyInterpreter().joinKey(e.f0);
            StreamSide side = cache.getKeyInterpreter().sideOf(e.f0);
            pendingWrites.put(e.f0, new WriteTask<K, T>(key, side, e.f1));
        }
    }

    @Override
    protected boolean isHotOrCanBecomeWithoutBackendAccess(K compositeKey, Cache<K, T> cache, StreamSide side, long ts) {
        return cache.keyIsInCache(compositeKey) || tryServeFromWriteBuffer(compositeKey, ts, cache);
    }

    // ---------- inner classes ----------
    /** Buffered write-back for an evicted cache entry. Immutable. */
    protected static final class WriteTask<K, T> {
        final K key; // not composite
        final StreamSide side;
        final List<T> values;
        WriteTask(K key, StreamSide side, List<T> values) {
            this.key = key; this.side = side; this.values = values;
        }
    }
}
