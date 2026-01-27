package net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.join;

import net.michaelkoepf.spegauge.api.common.model.sqbench.EventWithTimestamp;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.LatencyTracker;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.cache.OperatorWithCacheHelper;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class SingleInputJoin<T extends EventWithTimestamp> extends KeyedProcessFunction<Long, T, Tuple2<T, T>> {
    public transient EventTypeHelper<T> eventTypeHelper;

    protected final long windowSize;
    protected final long windowSlide;

    protected transient ListState<Tuple2<T, Long>> stream1State;
    protected transient ListState<Tuple2<T, Long>> stream2State;

    private transient HashMap<Long, Long> windowTriggers; // key -> trigger ts

    private boolean nexmark;

    protected transient LatencyTracker latencyTracker1, latencyTracker2;

    public SingleInputJoin(long windowSize, long windowSlide, boolean nexmark) {
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
        windowTriggers = new HashMap<>();
        System.out.println("streamIndex,time,taskIndex,average,p50,p90,p95,p99,p999");
    }

    @Override
    public void processElement(T value, Context ctx,
                                Collector<Tuple2<T, T>> out) throws Exception {
        if (eventTypeHelper.elemOfTypeA(value)) {
            processElementA(value, ctx, out);
        } else if (eventTypeHelper.elemOfTypeB(value)) {
            processElementB(value, ctx, out);
        } else {
            throw new RuntimeException("Unknown type of entity");
        }
    }

    public void processElementA(T value, Context ctx,
                                Collector<Tuple2<T, T>> out) throws Exception {
        long startTime = value.processingTimeMilliSecondsStart;
        processElement(value, ctx, out, stream1State, stream2State, true);
        latencyTracker1.updateLatency(1, startTime, ctx.timestamp(), getRuntimeContext().getTaskInfo().getIndexOfThisSubtask());
    }

    public void processElementB(T value, Context ctx,
                                Collector<Tuple2<T, T>> out) throws Exception {
        long startTime = value.processingTimeMilliSecondsStart;
        processElement(value, ctx, out, stream2State, stream1State, false);
        latencyTracker2.updateLatency(2, startTime, ctx.timestamp(), getRuntimeContext().getTaskInfo().getIndexOfThisSubtask());
    }

    protected void processElement(T value, Context ctx,
                                  Collector<Tuple2<T, T>> out,
                                  ListState<Tuple2<T, Long>> stateSelf,
                                  ListState<Tuple2<T, Long>> otherState, boolean state1) throws Exception {
        long currentTime = ctx.timestamp();
        long key = ctx.getCurrentKey();
        OperatorWithCacheHelper.registerTimerForEvent(currentTime, windowSize, windowSlide, ctx);
        stateSelf.add(Tuple2.of(value, currentTime));

        joinTuple(value, key, currentTime, state1, otherState, out);
    }

    private void joinTuple(T tuple, long key, long currentTime, boolean state1,
                           ListState<Tuple2<T, Long>> otherStreamState,
                           Collector<Tuple2<T, T>> out) throws Exception {
        Iterable<Tuple2<T, Long>> otherState = otherStreamState.get();

        long mergedKey = mergedKeyStateId(key, !state1);
        if (windowTriggers.containsKey(mergedKey)) {
            List<Tuple2<T, Long>> newVal = removeExpiredElements(otherState, windowTriggers.get(mergedKey));
            otherStreamState.update(newVal);

            otherState = newVal;

            windowTriggers.remove(mergedKey);
        }

        for (Tuple2<T, Long> otherTuple : otherState) {
            if (currentTime - otherTuple.f1 <= windowSize) {
                out.collect(Tuple2.of(tuple, otherTuple.f0));
            }
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<T, T>> out) {
        long currentTime = ctx.timestamp();
        long key = ctx.getCurrentKey();

        windowTriggers.put(mergedKeyStateId(key, true), currentTime);
        windowTriggers.put(mergedKeyStateId(key, false), currentTime);
    }

    private List<Tuple2<T, Long>> removeExpiredElements(Iterable<Tuple2<T, Long>> state, long currentTime) {
        List<Tuple2<T, Long>> allElements = new ArrayList<>();
        long minTimestamp = currentTime - windowSize + windowSlide;
        for (Tuple2<T, Long> entry : state) {
            if (entry.f1 >= minTimestamp) {
                allElements.add(entry);
            }
        }
        return allElements;
    }

    private static long mergedKeyStateId(long key, boolean state1) {
        return (key << 1) | (state1 ? 1L : 0L);
    }
}
