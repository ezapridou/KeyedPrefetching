package net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.join;

import net.michaelkoepf.spegauge.api.common.model.sqbench.EventWithTimestamp;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.LatencyTracker;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.cache.CacheLRUAppend;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.cache.OperatorWithCacheHelper;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.prefetching.Prefetcher;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.sidehandling.SideHandle;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.sidehandling.StreamSide;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.internal.KeyAccessibleState;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.operators.InputSelectable;
import org.apache.flink.streaming.api.operators.InputSelection;
import org.apache.flink.util.Collector;

import java.util.*;
import java.util.logging.Logger;

public class SingleInputJoinPrefetching<T extends EventWithTimestamp>
        extends KeyedCoProcessFunction<Long, Tuple3<Long, Long, Boolean>, T, Tuple2<T, T>>
        implements InputSelectable {
    private transient EventTypeHelper<T> eventTypeHelper;

    private final long windowSize;
    private final long windowSlide;

    private transient ListState<Tuple2<T, Long>> stream1State;
    private transient ListState<Tuple2<T, Long>> stream2State;

    private KeyAccessibleState<Long, List<Tuple2<T, Long>>> stream1KeyAccessibleState;
    private KeyAccessibleState<Long, List<Tuple2<T, Long>>> stream2KeyAccessibleState;

    // size of the cache in number of tuples
    private final int cacheSize = 3000000;

    private static final Logger logger = Logger.getLogger(SingleInputJoinPrefetching.class.getName());

    private Prefetcher<Long, Tuple2<T, Long>> prefetcher;
    //private long lastNormalTupleTs = 0, lastHintTs = 0;

    private boolean nexmark;
    protected transient LatencyTracker latencyTracker1, latencyTracker2;

    private Map<Long, SingleInputJoinAsync.PendingTuple<T>> pendingTuples = new HashMap<>();

    public SingleInputJoinPrefetching(long windowSize, long windowSlide, boolean nexmark) {
        this.windowSize = windowSize;
        this.windowSlide = windowSlide;
        this.nexmark = nexmark;
    }

    @Override
    public void open(OpenContext parameters) {
        if (nexmark) {
            this.eventTypeHelper = (EventTypeHelper<T>)new JoinNexmarkEventHelper();
        } else {
            this.eventTypeHelper = (EventTypeHelper<T>)new JoinEntityRecordFullHelper();
        }

        latencyTracker1 = new LatencyTracker(2608);
        latencyTracker2 = new LatencyTracker(200000);

        String stateName1 = "stream1State";
        String stateName2 = "stream2State";
        stream1State = getRuntimeContext().getListState(eventTypeHelper.getStateDescriptor(stateName1));
        stream2State = getRuntimeContext().getListState(eventTypeHelper.getStateDescriptor(stateName2));

        var cache = new CacheLRUAppend<Long, Tuple2<T, Long>>(cacheSize, false, 1);

        stream1KeyAccessibleState = OperatorWithCacheHelper.getKeyAccessibleListState(stream1State, stateName1);
        stream2KeyAccessibleState = OperatorWithCacheHelper.getKeyAccessibleListState(stream2State, stateName2);

        prefetcher = new Prefetcher<Long, Tuple2<T, Long>>(cache, true);
        prefetcher.wire(StreamSide.FIRST, stream1KeyAccessibleState);
        prefetcher.wire(StreamSide.SECOND, stream2KeyAccessibleState);

        System.out.println("streamIndex,time,taskIndex,average,p50,p60,p70,p80,p90,p95,p99,p999,cacheSize,maxCacheSize");
    }

    @Override
    public void close() {
        prefetcher.shutdown();
    }

    // process metadata (hints)
    @Override
    public void processElement1(Tuple3<Long, Long, Boolean> value, Context ctx,
                                Collector<Tuple2<T, T>> out) {
        // prefetch the key
        long key = value.f0;
        long timestampWhenStateNeeded = value.f1;
        boolean stream1 = value.f2;

        StreamSide other = stream1 ? StreamSide.SECOND : StreamSide.FIRST;
        long compositeKey = SideHandle.mergedKeyStateId(key, other);

        // prefetch opposite side (async)
        if (prefetcher.cacheHasGottenFull) {
            prefetcher.fetchKeyAsync(other, compositeKey, key, timestampWhenStateNeeded, 1, false, windowSize, windowSlide);
        } else {
            prefetcher.readOrCreateEntryInCache(compositeKey, timestampWhenStateNeeded);
        }
    }

    // process data
    @Override
    public void processElement2(T value, Context ctx,
                                Collector<Tuple2<T, T>> out) {
        if (eventTypeHelper.elemOfTypeA(value)) {
            processElement(value, StreamSide.FIRST, StreamSide.SECOND, ctx, out, true, stream1State);
        } else if (eventTypeHelper.elemOfTypeB(value)) {
            processElement(value, StreamSide.SECOND, StreamSide.FIRST, ctx, out, false, stream2State);
        } else {
            throw new RuntimeException("Unknown type of entity");
        }
    }

    @Override
    public InputSelection nextSelection() {
        return InputSelection.ALL;
    }

    // TODO there is a correctness problem:
    // An upsert can get lost if it is being executed concurrently with an async fetch
    private void processElement(T value, StreamSide self, StreamSide other, Context ctx,
                                Collector<Tuple2<T, T>> out,
                                boolean elementBelongsToStream1, ListState<Tuple2<T, Long>> listStateSelf) {
        long currentTime = ctx.timestamp();
        long key = ctx.getCurrentKey();
        OperatorWithCacheHelper.registerTimerForEvent(currentTime, windowSize, windowSlide, ctx);

        while (prefetcher.hasCompletedFetchesForPending()) {
            long drainedCompositeKeyOther = prefetcher.drainOnePendingResult(windowSize, windowSlide);
            SingleInputJoinAsync.PendingTuple<T> pt = pendingTuples.remove(drainedCompositeKeyOther);
            joinStateInCache(drainedCompositeKeyOther, pt.tuple, pt.timestamp, out, pt.stream1);
        }

        long compositeKeySelf = SideHandle.mergedKeyStateId(key, self);
        long compositeKeyOther = SideHandle.mergedKeyStateId(key, other);

        // 1) upsert arriving tuple into SELF side
        prefetcher.upsertSelf(self, compositeKeySelf, value, currentTime, listStateSelf);

        // 2) ensure OTHER side is hot now (blocks if needed)
        boolean keyWasInCacheOther = prefetcher.fetchKeyAsync(other, compositeKeyOther, key, currentTime, 2, true,
                windowSize, windowSlide);

        // 3) join from the OTHER side cache
        if (keyWasInCacheOther) {
            joinStateInCache(compositeKeyOther, value, currentTime, out, elementBelongsToStream1);
        }
        else{
            pendingTuples.put(compositeKeyOther,
                    new SingleInputJoinAsync.PendingTuple<T>(key, value, currentTime, elementBelongsToStream1));
        }

        prefetcher.setLastProcessedTs(currentTime);

        // we do not want to drain prefetching results if pending results where drained so that we do not delay the next
        // record too much
        if (keyWasInCacheOther && prefetcher.hasCompletedFetches()) {
            long drainedKey = prefetcher.drainOneResult(windowSize, windowSlide); // drain some prefetching results
            if (pendingTuples.containsKey(drainedKey)){
                SingleInputJoinAsync.PendingTuple<T> pt = pendingTuples.remove(drainedKey);
                joinStateInCache(drainedKey, pt.tuple, pt.timestamp, out, pt.stream1);
            }
        }
        prefetcher.drainWrites();
    }

    private void joinStateInCache(long compositeKeyOther, T value, long timestamp,
                                  Collector<Tuple2<T, T>> out, boolean stream1) {
        List<Tuple2<T, Long>> otherStreamElems = prefetcher.getFromCache(compositeKeyOther, timestamp);
        if (otherStreamElems != null) {
            OperatorWithCacheHelper.joinTuple(value, timestamp, otherStreamElems, out, windowSize);
        }
        updLatency(value.processingTimeMilliSecondsStart, timestamp, stream1);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<T, T>> out) {
        long currentTime = ctx.timestamp();
        long key = ctx.getCurrentKey();

        long compositeKey1 = SideHandle.mergedKeyStateId(key, StreamSide.FIRST);
        long compositeKey2 = SideHandle.mergedKeyStateId(key, StreamSide.SECOND);

        if (prefetcher.isHot(compositeKey1)) {
            prefetcher.expireInCache(compositeKey1, currentTime, windowSize, windowSlide);
        }
        else {
            prefetcher.registerWindowTrigger(compositeKey1, currentTime);
        }
        if (prefetcher.isHot(compositeKey2)) {
            prefetcher.expireInCache(compositeKey2, currentTime, windowSize, windowSlide);
        }
        else {
            prefetcher.registerWindowTrigger(compositeKey2, currentTime);
        }
    }

    private void updLatency(long startTime, long currentTs, boolean stream1) {
        if (stream1) {
            latencyTracker1.updateLatency(1, startTime, currentTs,
                    getRuntimeContext().getTaskInfo().getIndexOfThisSubtask(), prefetcher.getCacheSize(),
                    prefetcher.getMaxCacheSize(),
                    prefetcher.getPrefetchingMisses(), prefetcher.getPrefetchingHits());
        }
        else
            latencyTracker2.updateLatency(2, startTime, currentTs, getRuntimeContext().getTaskInfo().getIndexOfThisSubtask());
    }
}
