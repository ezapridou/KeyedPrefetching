package net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.join;

import net.michaelkoepf.spegauge.api.common.model.sqbench.EventWithTimestamp;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.cache.CacheAppend;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.cache.CacheClockAppend;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.cache.CacheLRUAppend;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.cache.OperatorWithCacheHelper;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.sidehandling.SideHandle;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.sidehandling.StreamSide;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.internal.KeyAccessibleState;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.List;

public class SingleInputJoinCache<T extends EventWithTimestamp> extends SingleInputJoin<T>{

    private KeyAccessibleState<Long, List<Tuple2<T, Long>>> stream1KeyAccessibleState;
    private KeyAccessibleState<Long, List<Tuple2<T, Long>>> stream2KeyAccessibleState;

    private transient CacheAppend<Long, Tuple2<T, Long>> cache;

    private final int cacheSize; // in number of tuples
    private final int cacheType = 1; // 0 = TopKeys, 1 = LRU, 2 = Clock

    private transient HashMap<Long, Long> windowTriggers; // key -> trigger ts

    public SingleInputJoinCache(long windowSize, long windowSlide, boolean nexmark) {
        super(windowSize, windowSlide, nexmark);
        this.cacheSize = 3000000; //1600000; 64g per tm
    }

    @Override
    public void open(OpenContext parameters) {
        super.open(parameters);
        System.out.println("streamIndex,time,taskIndex,average,p50,p90,p95,p99,p999,cacheSize,maxCacheSize");

        if (cacheType == 0) {
            // TopKeys cache
            throw new RuntimeException("TopKeys cache not supported now.");
        }
        else if (cacheType == 1) {
            // LRU cache
            cache = new CacheLRUAppend(cacheSize, false, 1);
        }
        else if (cacheType == 2) {
            // Clock cache
            cache = new CacheClockAppend(cacheSize, false, 1);
        }
        else {
            throw new RuntimeException("Unknown cache type " + cacheType);
        }

        windowTriggers = new HashMap<>();

        stream1KeyAccessibleState = OperatorWithCacheHelper.getKeyAccessibleListState(stream1State, "stream1State");
        stream2KeyAccessibleState = OperatorWithCacheHelper.getKeyAccessibleListState(stream2State, "stream2State");
    }

    @Override
    public void processElement(T value, Context ctx,
                                Collector<Tuple2<T, T>> out) throws Exception {
        if (eventTypeHelper.elemOfTypeA(value)) {
            processElementA(value, ctx, out);
        } else if (eventTypeHelper.elemOfTypeB(value)) {
            processElementB(value, ctx, out);
        } else {
            throw new RuntimeException("Unknown type ");
        }
    }

    @Override
    public void processElementA(T value, Context ctx,
                                Collector<Tuple2<T, T>> out) {
        long startTime = value.processingTimeMilliSecondsStart;
        processElement(value, ctx, out, cache, StreamSide.FIRST, StreamSide.SECOND, stream1KeyAccessibleState,
                stream2KeyAccessibleState, stream1State);
        latencyTracker1.updateLatency(1, startTime, ctx.timestamp(),
                getRuntimeContext().getTaskInfo().getIndexOfThisSubtask(), cache.getCacheSize(), cache.getMaxCacheSize());
    }

    @Override
    public void processElementB(T value, Context ctx, Collector<Tuple2<T, T>> out) {
        long startTime = value.processingTimeMilliSecondsStart;
        processElement(value, ctx, out, cache, StreamSide.SECOND, StreamSide.FIRST, stream2KeyAccessibleState,
                stream1KeyAccessibleState, stream2State);
        latencyTracker2.updateLatency(2, startTime, ctx.timestamp(), getRuntimeContext().getTaskInfo().getIndexOfThisSubtask());
    }

    private void processElement(T value, Context ctx, Collector<Tuple2<T, T>> out,
                                CacheAppend<Long, Tuple2<T, Long>> cache, StreamSide selfSide, StreamSide otherSide,
                                KeyAccessibleState<Long, List<Tuple2<T, Long>>> stateSelf,
                                KeyAccessibleState<Long, List<Tuple2<T, Long>>> otherState,
                                ListState<Tuple2<T, Long>> listStateSelf){
        long currentTime = ctx.timestamp();
        long key = ctx.getCurrentKey();
        OperatorWithCacheHelper.registerTimerForEvent(currentTime, windowSize, windowSlide, ctx);

        final long compositeKeySelf  = SideHandle.mergedKeyStateId(key, selfSide);
        final long compositeKeyOther = SideHandle.mergedKeyStateId(key, otherSide);

        insertTuple(compositeKeySelf, value, currentTime, cache, stateSelf, listStateSelf);

        List<Tuple2<T, Long>> otherStreamElems = OperatorWithCacheHelper.getStateCache(
                compositeKeyOther, key, otherState, cache, currentTime);

        if (windowTriggers.containsKey(compositeKeyOther)) {
            long triggerTs = windowTriggers.get(compositeKeyOther);
            OperatorWithCacheHelper.removeExpiredElementsCache(cache, triggerTs, compositeKeyOther, windowSize, windowSlide);
            windowTriggers.remove(compositeKeyOther);
        }

        OperatorWithCacheHelper.joinTuple(value, currentTime, otherStreamElems, out, windowSize);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<T, T>> out) {
        long currentTime = ctx.timestamp();
        long key = ctx.getCurrentKey();

        long compositeKey1 = SideHandle.mergedKeyStateId(key, StreamSide.FIRST);
        long compositeKey2 = SideHandle.mergedKeyStateId(key, StreamSide.SECOND);

        if (cache.keyIsInCache(compositeKey1)) {
            OperatorWithCacheHelper.removeExpiredElementsCache(cache, currentTime, compositeKey1, windowSize, windowSlide);
        }
        else {
            windowTriggers.put(compositeKey1, currentTime);
        }
        if (cache.keyIsInCache(compositeKey2)) {
            OperatorWithCacheHelper.removeExpiredElementsCache(cache, currentTime, compositeKey2, windowSize, windowSlide);
        }
        else {
            windowTriggers.put(compositeKey2, currentTime);
        }
    }

    private void insertTuple(long compositeKey, T value, long currentTime, CacheAppend<Long, Tuple2<T, Long>> cache,
                             KeyAccessibleState<Long, List<Tuple2<T, Long>>> keyAccState,
                             ListState<Tuple2<T, Long>> state) {
        if (cache.keyIsInCache(compositeKey)) {
            OperatorWithCacheHelper.insertTupleToCache(compositeKey, value, currentTime, cache, keyAccState);
        }
        else {
            try {
                state.add(Tuple2.of(value, currentTime));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
