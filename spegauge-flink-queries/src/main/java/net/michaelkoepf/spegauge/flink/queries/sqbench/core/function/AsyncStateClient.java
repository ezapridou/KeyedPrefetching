package net.michaelkoepf.spegauge.flink.queries.sqbench.core.function;

import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.cache.Cache;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.cache.CacheAppend;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.cache.OperatorWithCacheHelper;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.sidehandling.SideHandle;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.sidehandling.StreamSide;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.internal.KeyAccessibleState;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

abstract public class AsyncStateClient<K, T> {

    // ---------- wiring ----------
    protected final EnumMap<StreamSide, SideHandle<K, T>> handles = new EnumMap<>(StreamSide.class);

    // ---------- execution infra ----------
    public final Cache<K, T> cache;
    /** Prefetch executor with priority support. */
    protected transient final PriorityThreadPoolExecutor fetchPool;
    /** Queue of completed prefetch results (raw bytes). */
    protected transient final ConcurrentLinkedQueue<FetchResult<K, T>> fetchResultQueue;

    /** Queue of completed fetches for pending tuples (raw bytes). */
    protected transient final ConcurrentLinkedQueue<FetchResult<K, T>> fetchResultQueuePending;
    /** Tracks keys currently being fetched: compositeKey -> latest timestamp when state is needed. */
    //protected transient final HashMap<K, Long> inFlightByKey;
    /** Futures for in-flight fetches: compositeKey -> future. */
    protected transient final Map<K, InFlightInfo> inFlightRequests;
    /** Window eviction triggers*/
    protected transient final HashMap<K, Long> windowTriggers; // key -> trigger ts

    // ---------- metrics ----------
    public long prefetchingHits = 0;
    public long prefetchingMisses = 0;

    // ---------- config ----------
    private final int threadPoolSize = 2;

    /** Global priority FIFO tiebreaker. */
    private static long seqGen = 0;

    public AsyncStateClient(Cache<K, T> cache, boolean windowing) {
        this.fetchPool = new PriorityThreadPoolExecutor(threadPoolSize);
        this.fetchResultQueue = new ConcurrentLinkedQueue<>();
        this.fetchResultQueuePending = new ConcurrentLinkedQueue<>();
        //this.inFlightByKey = new HashMap<>();
        if (windowing)
            this.windowTriggers = new HashMap<>();
        else
            this.windowTriggers = new HashMap<>(1);
        inFlightRequests = new ConcurrentHashMap<>();
        this.cache = cache;
    }

    public void shutdown() {
        fetchPool.shutdown();
    }

    /**
     * Wire one side with its cache and keyed state. Call from operator open().
     * For joins, wire both FIRST and SECOND. For single-input ops, wire PRIMARY only.
     */
    public void wire(StreamSide side, KeyAccessibleState<K, List<T>> state) {
        if (side == null || state == null) {
            throw new IllegalArgumentException("StreamSide and state must be non-null");
        }
        handles.put(side, new SideHandle<K, T>(state));
    }

    // ---------- operator-facing API ----------
    // function that returns true if there are any completed fetches
    public boolean hasCompletedFetchesForPending() {
        return !fetchResultQueuePending.isEmpty();
    }

    public boolean hasCompletedFetches() {
        return !fetchResultQueue.isEmpty();
    }

    /** Drain fetch results and materialize them into the caches. */
    public K drainOnePendingResult(long windowSize, long windowSlide) {
        FetchResult<K, T> r = fetchResultQueuePending.poll();
        processFetchResult(r, windowSize, windowSlide);
        return cache.getKeyInterpreter().encode(r.key, r.side);
    }

    public K drainOneResult(long windowSize, long windowSlide) {
        FetchResult<K, T> r = fetchResultQueue.poll();
        processFetchResult(r, windowSize, windowSlide);
        return cache.getKeyInterpreter().encode(r.key, r.side);
    }

    /** Try to make key hot soon (async). Returns true if it's already hot now. */
    public boolean fetchKeyAsync(StreamSide side, K compositeKey, K key, long ts, int priority, boolean pendingTuple,
                                 long windowSize, long windowSlide) {
        if (isHotOrCanBecomeWithoutBackendAccess(compositeKey, cache, side, ts)) {
            cache.touch(compositeKey, ts);
            return true;
        }
        Future<FetchResult<K, T>> fetchReq  = requestPrefetch(compositeKey, key, side, ts, pendingTuple, priority);
        // if the fetch already finished and was enqueued to the non-pending queue,
        // promote its result to the pending queue so TopN can consume it.
        if (pendingTuple && fetchReq.isDone()) {
            try {
                FetchResult<K, T> res = fetchReq.get();
                // TODO I can store in inFlight which queue it is in to avoid this search
                if (!fetchResultQueue.remove(res))
                    fetchResultQueuePending.remove(res);
                processFetchResult(res, windowSize, windowSlide);
                //fetchResultQueuePending.add(fut.get()); // safe: non-blocking because isDone() is true
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            } catch (ExecutionException ee) {
                throw new RuntimeException(ee);
            }
            return true;
        }

        return false;
    }

    public boolean fetchKeyAsync(StreamSide side, K compositeKey, K key, long ts, boolean pendingTuple,
                                 long windowSize, long windowSlide){
        return fetchKeyAsync(side, compositeKey, key, ts, 1, pendingTuple, windowSize, windowSlide);
    }

    /** Make key present in cache now (if the key is not already in the cache we wait until it is fetched).
     *  Returns true if key was already hot.
     */
    public boolean bringKeyToCacheSync(StreamSide side, K compositeKey, K key, long ts, long windowSize, long windowSlide) {
        if (isHotOrCanBecomeWithoutBackendAccess(compositeKey, cache, side, ts)) {
            cache.touch(compositeKey, ts);
            prefetchingHits++;
            return true;
        }
        fetchStateToCacheSync(compositeKey, key, side, ts, windowSize, windowSlide);
        return false;
    }

    /** Upsert an element: insert into cache if hot, else append to backend state. */
    public <V> boolean upsertSelf(StreamSide side, K compositeKey, V value, long ts, ListState<Tuple2<V, Long>> listState) {
        if (!(cache instanceof CacheAppend)) {
            throw new IllegalStateException("Cache is not append-capable. Upsert not supported");
        }
        SideHandle<K, Tuple2<V, Long>> h = (SideHandle<K, Tuple2<V, Long>>)requireHandle(side);
        if (isHotOrCanBecomeWithoutBackendAccess(compositeKey, cache, side, ts)) {
            OperatorWithCacheHelper.insertTupleToCache(compositeKey, value, ts, (CacheAppend<K, Tuple2<V, Long>>) cache, h.state);
            return true;
        } else {
            try { listState.add(Tuple2.of(value, ts)); } catch (Exception e) { throw new RuntimeException(e); }
            return false;
        }
    }

    /** Read cached list (never null). Does NOT trigger a fetch. */
    public List<T> readOrCreateEntryInCache(K compositeKey, long ts) {
        return cache.computeIfAbsent(compositeKey, ts);
    }

    public List<T> getFromCache(K compositeKey, long ts) {
        return cache.get(compositeKey, ts);
    }

    public boolean isHot(K compositeKey) { return cache.keyIsInCache(compositeKey); }

    public void expireInCache(K compositeKey, long triggerTs, long windowSize, long windowSlide) {
        if (!(cache instanceof CacheAppend)) {
            throw new IllegalStateException("Cache is not append-capable. Expire not supported");
        }
        OperatorWithCacheHelper.removeExpiredElementsCache((CacheAppend<K, ?>)cache, triggerTs, compositeKey, windowSize, windowSlide);
    }

    public void registerWindowTrigger(K compositeKey, long triggerTs) {
        windowTriggers.put(compositeKey, triggerTs);
    }

    // ---------- metrics ----------
    public long getPrefetchingHits() { return prefetchingHits; }
    public long getPrefetchingMisses() { return prefetchingMisses; }
    public void clearHitsAndMisses() { prefetchingHits = 0; prefetchingMisses = 0; }
    public int getCacheSize() { return cache.getCacheSize(); }
    public int getMaxCacheSize() { return cache.getMaxCacheSize(); }

    // ---------- internal I/O paths ----------

    /** Sync state request and add result to cache immediately */
    protected void fetchStateToCacheSync(K compositeKey, K key, StreamSide side, long currentTime, long windowSize, long windowSlide) {
        throw new UnsupportedOperationException("Synchronous fetch not implemented in this AsyncStateClient variant");
        /*
        Future<FetchResult<K, T>> fetchReq = requestPrefetch(compositeKey, key, side, currentTime, true, true, 2);

        try {
            FetchResult<K, T> res = fetchReq.get();
            // TODO this is relatively expensive. An optimization would be to not add it
            //  if I know I will process it immediately.
            fetchResultQueue.remove(res);
            processFetchResult(res, windowSize, windowSlide);
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }*/
    }

    /** Async state request */
    protected Future<FetchResult<K, T>> requestPrefetch(K compositeKey, K key, StreamSide side, long timestamp,
                                                   boolean pendingTuple, int priority) {
        SideHandle<K, T> h = requireHandle(side);

        // Fast deduplication: if already in-flight, update timestamp and return same future.
        if (inFlightRequests.containsKey(compositeKey)) {
            InFlightInfo<K, T> info = inFlightRequests.get(compositeKey);
            if (info.latestNeededTs < timestamp) {
                info.latestNeededTs = timestamp;
            }
            if (pendingTuple) {
                prefetchingHits++;
                info.hasPendingWaiter = true;
            }
            return info.future;
        }
        if (pendingTuple){
            prefetchingMisses++;
        }

        InFlightInfo<K, T> info = new InFlightInfo(timestamp, pendingTuple, null);
        inFlightRequests.put(compositeKey, info);

        // async path via pool
        info.future = fetchPool.submit(priority, () -> {
            try {
                List<T> stateOfKey = h.state.get(key);//h.state.getRawBytes(key);
                FetchResult<K, T> fetchResult = new FetchResult<K, T>(key, stateOfKey, side);
                InFlightInfo infoOfKey = inFlightRequests.get(compositeKey);
                if ((infoOfKey != null && infoOfKey.hasPendingWaiter) || pendingTuple){
                    fetchResultQueuePending.add(fetchResult);
                } else {
                    fetchResultQueue.add(fetchResult);
                }
                return fetchResult;
            } catch (Throwable t) {
                throw new RuntimeException(t);
            }
        });

        return info.future;
    }

    abstract protected void processFetchResult(FetchResult<K, T> fetchResult, long windowSize, long windowSlide);

    protected boolean isHotOrCanBecomeWithoutBackendAccess(K compositeKey, Cache<K, T> cache, StreamSide side, long ts) {
        return cache.keyIsInCache(compositeKey);
    }

    // ---------- helpers ----------

    protected SideHandle<K, T> requireHandle(StreamSide side) {
        SideHandle<K, T> h = handles.get(side);
        if (h == null) throw new IllegalStateException("Side not wired: " + side);
        return h;
    }

    // ---------- inner classes ----------

    /** Result of a raw state fetch. */
    public static final class FetchResult<K, T> {
        public final K key; // not composite
        public final List<T> state;
        public final StreamSide side;
        public FetchResult(K key, List<T> state, StreamSide side) { this.key = key; this.state = state; this.side = side;}
    }

    public static final class InFlightInfo<K,T> {
        public Future<FetchResult<K, T>> future;
        public long latestNeededTs;
        public boolean hasPendingWaiter; // true if at least one tuple is blocked on this fetch
        public InFlightInfo(long latestNeededTs, boolean hasPendingWaiter, Future<FetchResult<K, T>> future) {
            this.latestNeededTs = latestNeededTs;
            this.hasPendingWaiter = hasPendingWaiter;
            this.future = future;
        }
    }

    /** RunnableFuture with priority + FIFO ordering among equals. Used for prioritizing prefetching tasks */
    protected static final class PriorityTask<V> extends FutureTask<V> implements Comparable<PriorityTask<?>> {
        private final int priority;      // higher value = higher priority (tweak as you like)
        private final long seq;          // FIFO among equal priorities

        PriorityTask(Callable<V> c, int priority) {
            super(c);
            this.priority = priority;
            this.seq = seqGen++;
        }

        PriorityTask(Runnable r, V result, int priority) {
            super(r, result);
            this.priority = priority;
            this.seq = seqGen++;
        }

        @Override
        public int compareTo(PriorityTask<?> other) {
            int byPriority = Integer.compare(other.priority, this.priority); // higher first
            return (byPriority != 0) ? byPriority : Long.compare(this.seq, other.seq); // FIFO tie-break
        }
    }

    /**ThreadPoolExecutor that produces PriorityTasks so the queue can prioritize submissions*/
    protected static final class PriorityThreadPoolExecutor extends ThreadPoolExecutor {
        PriorityThreadPoolExecutor(int poolSize) {
            // TODO PriorityBlockingQueue is unbounded; consider monitoring or wrapping.
            super(poolSize, poolSize, 60L, TimeUnit.SECONDS, new PriorityBlockingQueue<>());
        }

        // Default: equal priority for all tasks (0)
        @Override
        protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
            return new PriorityTask<>(callable, 0);
        }

        @Override
        protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
            return new PriorityTask<>(runnable, value, 0);
        }

        // priority-aware submit overloads
        public <T> Future<T> submit(int priority, Callable<T> task) {
            PriorityTask<T> pt = new PriorityTask<>(task, priority);
            execute(pt);
            return pt;
        }

        public Future<?> submit(int priority, Runnable task) {
            PriorityTask<?> pt = new PriorityTask<>(task, null, priority);
            execute(pt);
            return pt;
        }
    }
}
