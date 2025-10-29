package net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.join;

import net.michaelkoepf.spegauge.api.common.model.sqbench.EventWithTimestamp;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.AsyncBaseline;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.LatencyTracker;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.cache.CacheLRUAppend;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.cache.OperatorWithCacheHelper;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.sidehandling.SideHandle;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.sidehandling.StreamSide;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.internal.KeyAccessibleState;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.*;
import java.util.logging.Logger;

public class SingleInputJoinAsync<T extends EventWithTimestamp>
        extends KeyedProcessFunction<Long, T, Tuple2<T, T>> {
    private transient EventTypeHelper<T> eventTypeHelper;

    private final long windowSize;
    private final long windowSlide;

    private transient ListState<Tuple2<T, Long>> stream1State;
    private transient ListState<Tuple2<T, Long>> stream2State;

    private KeyAccessibleState<Long, List<Tuple2<T, Long>>> stream1KeyAccessibleState;
    private KeyAccessibleState<Long, List<Tuple2<T, Long>>> stream2KeyAccessibleState;

    private final int cacheSize = 3000000; // size of the cache in number of tuples

    private static final Logger logger = Logger.getLogger(SingleInputJoinAsync.class.getName());

    private AsyncBaseline<Long, Tuple2<T, Long>> asyncStateClient;

    private Map<Long, PendingTuple<T>> pendingTuples = new HashMap<>();

    // SingleInputJoinAsync fields
    private static final long DRAIN_INTERVAL_MS = 5;
    private transient volatile boolean drainTimerArmed = false;

    private final boolean nexmark;
    protected transient LatencyTracker latencyTracker1, latencyTracker2;

    public static class PendingTuple<T> {
        public final long key; // non-composite key
        public final T tuple;
        public final long timestamp;
        public final boolean stream1;

        public PendingTuple(long key, T tuple, long timestamp, boolean stream1) {
            this.key = key;
            this.tuple = tuple;
            this.timestamp = timestamp;
            this.stream1 = stream1;
        }
    }

    public SingleInputJoinAsync(long windowSize, long windowSlide, boolean nexmark) {
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

        var cache = new CacheLRUAppend<Long, Tuple2<T, Long>>((int) Math.floor(cacheSize), false, 1);

        stream1KeyAccessibleState = OperatorWithCacheHelper.getKeyAccessibleListState(stream1State, stateName1);
        stream2KeyAccessibleState = OperatorWithCacheHelper.getKeyAccessibleListState(stream2State, stateName2);

        asyncStateClient = new AsyncBaseline<Long, Tuple2<T, Long>>(cache, true);
        asyncStateClient.wire(StreamSide.FIRST, stream1KeyAccessibleState);
        asyncStateClient.wire(StreamSide.SECOND, stream2KeyAccessibleState);

        System.out.println("streamIndex,time,taskIndex,average,p50,p60,p70,p80,p90,p95,p99,p999,cacheSize,maxCacheSize");
    }

    @Override
    public void close() {
        asyncStateClient.shutdown();
    }

    // process data
    @Override
    public void processElement(T value, Context ctx,
                                Collector<Tuple2<T, T>> out) {
        //ensureDrainTimer(ctx);
        if (eventTypeHelper.elemOfTypeA(value)) {
            processElement(value, StreamSide.FIRST, StreamSide.SECOND, ctx, out, true, stream1State);
        } else if (eventTypeHelper.elemOfTypeB(value)) {
            processElement(value, StreamSide.SECOND, StreamSide.FIRST, ctx, out, false, stream2State);
        } else {
            throw new RuntimeException("Unknown type of entity");
        }
    }

    // TODO there is a correctness problem:
    // k1 from stream1 arrives. Then we do the upsert k1 into stream1 state. Then we fetch async k1 from stream2.
    // While the fetch is happening, k1 from stream2 arrives. We upsert k1 into stream2 state.
    // 2 bad things can happen:
    // 1) the fetch from stream2 arrives and includes k1 in stream2. Then the two tuples will be joined twice
    // 2) the fetch from stream2 arrives and does not include k1 in stream2. Then the upsert will get lost.
    // To avoid this I should not allow concurrent requests for the same key.
    private void processElement(T value, StreamSide self, StreamSide other, Context ctx,
                                Collector<Tuple2<T, T>> out,
                                boolean elementBelongsToStream1, ListState<Tuple2<T, Long>> listStateSelf) {
        long currentTime = ctx.timestamp();
        long key = ctx.getCurrentKey();
        OperatorWithCacheHelper.registerTimerForEvent(currentTime, windowSize, windowSlide, ctx);

        // 1) check if any fetches are completed and drain them
        while (asyncStateClient.hasCompletedFetchesForPending()) {
            long drainedCompositeKeyOther = asyncStateClient.drainOnePendingResult(windowSize, windowSlide);
            PendingTuple<T> pt = pendingTuples.remove(drainedCompositeKeyOther);
            joinStateInCache(drainedCompositeKeyOther, pt.tuple, pt.timestamp, out, pt.stream1);
        }

        long compositeKeySelf = SideHandle.mergedKeyStateId(key, self);
        long compositeKeyOther = SideHandle.mergedKeyStateId(key, other);

        // 2) upsert arriving tuple into SELF side
        asyncStateClient.upsertSelf(self, compositeKeySelf, value, currentTime, listStateSelf);

        // 3) if key is hot do the join otherwise trigger fetch
        boolean hot = asyncStateClient.fetchKeyAsync(other, compositeKeyOther, key, currentTime, true, windowSize, windowSlide);
        if (hot) {
            joinStateInCache(compositeKeyOther, value, currentTime, out, elementBelongsToStream1);
        } else {
            // remember the tuple for later processing
            // creating the mergedKey with the other side to match the requests in AsyncBaseline
            pendingTuples.put(compositeKeyOther,
                    new PendingTuple<T>(key, value, currentTime, elementBelongsToStream1));
        }
    }

    private void joinStateInCache(long compositeKeyOther, T value, long timestamp,
                                  Collector<Tuple2<T, T>> out, boolean stream1) {
        List<Tuple2<T, Long>> otherStreamElems = asyncStateClient
                .getFromCache(compositeKeyOther, timestamp);
        if (otherStreamElems != null) {
            OperatorWithCacheHelper.joinTuple(value, timestamp, otherStreamElems, out, windowSize);
        }
        updLatency(value.processingTimeMilliSecondsStart, timestamp, stream1);

        // if cache disabled drop the other side's cached state immediately
        if (!asyncStateClient.isCacheEnabled()) {
            asyncStateClient.dropCachedState(compositeKeyOther);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<T, T>> out) {
        /*if (ctx.timeDomain() == TimeDomain.PROCESSING_TIME) {
            while (asyncStateClient.hasCompletedFetches()) {
                long drainedMergedKey = asyncStateClient.drainOneResult(windowSize, windowSlide);
                PendingTuple pt = pendingTuples.remove(drainedMergedKey);
                StreamSide otherSidePt  = pt.stream1 ? StreamSide.SECOND : StreamSide.FIRST;
                joinStateInCache(pt.key, pt.tuple, pt.timestamp, otherSidePt, out, pt.stream1);
            }
            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + DRAIN_INTERVAL_MS);
        }*/

        long currentTime = ctx.timestamp();
        long key = ctx.getCurrentKey();

        long compositeKey1 = SideHandle.mergedKeyStateId(key, StreamSide.FIRST);
        long compositeKey2 = SideHandle.mergedKeyStateId(key, StreamSide.SECOND);

        //JoinWithCacheHelper.onTimer(
        //      currentTime, key, windowSize, windowSlide,
        //      cacheStream1, stream1State, cacheStream2, stream2State);

        // timer for window expiration
        //if (ctx.timeDomain() == TimeDomain.EVENT_TIME) {
            if (asyncStateClient.isHot(compositeKey1)) {
                asyncStateClient.expireInCache(compositeKey1, currentTime, windowSize, windowSlide);
            } else {
                //removeExpiredElements(stream1State, timestamp, windowSize, windowSlide);
                asyncStateClient.registerWindowTrigger(compositeKey1, currentTime);
            }
            if (asyncStateClient.isHot(compositeKey2)) {
                asyncStateClient.expireInCache(compositeKey2, currentTime, windowSize, windowSlide);
            } else {
                //removeExpiredElements(stream2State, currentTime, windowSize, windowSlide);
                asyncStateClient.registerWindowTrigger(compositeKey2, currentTime);
            }
        //}
    }

    private void ensureDrainTimer(Context ctx) {
        if (!drainTimerArmed) {
            drainTimerArmed = true;
            long now = ctx.timerService().currentProcessingTime();
            ctx.timerService().registerProcessingTimeTimer(now + DRAIN_INTERVAL_MS);
        }
    }

    private void updLatency(long startTime, long currentTs, boolean stream1) {
        if (stream1)
            latencyTracker1.updateLatency(1, startTime, currentTs,
                    getRuntimeContext().getTaskInfo().getIndexOfThisSubtask(), asyncStateClient.getCacheSize(),
                    asyncStateClient.getMaxCacheSize());
        else
            latencyTracker2.updateLatency(2, startTime, currentTs, getRuntimeContext().getTaskInfo().getIndexOfThisSubtask());
    }
}
