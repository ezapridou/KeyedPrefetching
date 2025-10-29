package net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.sideinputjoin;

import net.michaelkoepf.spegauge.api.common.model.sqbench.NexmarkEvent;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.LatencyTracker;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.cache.CacheLRU;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.cache.OperatorWithCacheHelper;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.prefetching.Prefetcher;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.sidehandling.StreamSide;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.topN.TopN_Async;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.internal.KeyAccessibleState;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.operators.InputSelectable;
import org.apache.flink.streaming.api.operators.InputSelection;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JoinSideInputPrefetching
        extends KeyedCoProcessFunction<Long, Tuple2<Long, Long>, NexmarkEvent, Tuple2<NexmarkEvent, String>>
        implements InputSelectable {
    protected transient ValueState<List<String>> staticTable;
    protected transient KeyAccessibleState<Long, List<String>> keyAccessibleState;

    protected transient LatencyTracker latencyTracker;

    protected KeyGroupRange keyGroupRange;
    protected int maxParallelism = 192;

    private final int cacheSize = 93750; // (20gb/24 task slots/8000 value size)*0.9

    private Prefetcher<Long, String> prefetcher;
    //private long lastNormalTupleTs, lastHintTs = 0;

    private StreamSide side = StreamSide.FIRST; // only one input, so always FIRST

    private long windowSize = Long.MAX_VALUE, windowSlide = 0; // no expiration of entries

    private Map<Long, JoinSideInputAsync.PendingTuple> pendingTuples = new HashMap<>();


    @Override
    public void open(OpenContext parameters) throws Exception {
        latencyTracker = new LatencyTracker(200000);
        String stateName = "staticTable";
        staticTable = getRuntimeContext().getState(new ValueStateDescriptor<List<String>>(stateName, Types.LIST(Types.STRING)));
        System.out.println("streamIndex,time,taskIndex,average,p50,p60,p70,p80,p90,p95,p99,p999");
        keyAccessibleState = OperatorWithCacheHelper.getKeyAccessibleValueState(staticTable, stateName);

        int operatorIndex = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        int parallelism = 48;
        int start = ((operatorIndex * maxParallelism + parallelism - 1) / parallelism);
        int end = ((operatorIndex + 1) * maxParallelism - 1) / parallelism;
        keyGroupRange = new KeyGroupRange(start, end);

        JoinSideInput.fillStaticTable(keyGroupRange, maxParallelism, keyAccessibleState, operatorIndex);

        var cache = new CacheLRU<Long, String>(cacheSize, true);

        prefetcher = new Prefetcher<Long, String>(cache, false);
        prefetcher.wire(side, keyAccessibleState);
    }

    @Override
    public void close() {
        prefetcher.shutdown();
    }

    @Override
    public InputSelection nextSelection() {
       // return lastHintTs > lastNormalTupleTs + 250000 ? InputSelection.FIRST : InputSelection.SECOND;
        return InputSelection.ALL;
    }

    @Override
    public void processElement1(Tuple2<Long, Long> hint, Context ctx, Collector<Tuple2<NexmarkEvent, String>> out) {
        long key = hint.f0;
        long timestampWhenStateNeeded = hint.f1;

        //lastHintTs = Math.max(lastHintTs, timestampWhenStateNeeded);

        prefetcher.fetchKeyAsync(side, key, key, timestampWhenStateNeeded, 1, false, windowSize, windowSlide);
    }

    @Override
    public void processElement2(NexmarkEvent value, Context ctx, Collector<Tuple2<NexmarkEvent, String>> out) {
        long key = ctx.getCurrentKey();
        long currentTime = ctx.timestamp();

        while(prefetcher.hasCompletedFetchesForPending()){
            long drainedKey = prefetcher.drainOnePendingResult(windowSize, windowSlide);
            JoinSideInputAsync.PendingTuple pt = pendingTuples.remove(drainedKey);
            enrichTuple(drainedKey, pt.tuple, pt.timestamp, out);
        }

        // ensure state is in cache now
        boolean keyWasInCache = prefetcher.fetchKeyAsync(side, key, key, currentTime, 2, true, windowSize, windowSlide);

        if (keyWasInCache) {
            enrichTuple(key, value, currentTime, out);
        }
        else {
            // remember this tuple for later processing
            pendingTuples.put(key, new JoinSideInputAsync.PendingTuple(key, value, currentTime));
        }

        prefetcher.setLastProcessedTs(currentTime);

        // we do not want to drain prefetching results if pending results where drained so that we do not delay the next
        // record too much
        if ( keyWasInCache && prefetcher.hasCompletedFetches()) { // !didDrainPending
            long drainedKey = prefetcher.drainOneResult(windowSize, windowSlide); // drain some prefetching results
            if (pendingTuples.containsKey(drainedKey)){
                JoinSideInputAsync.PendingTuple pt = pendingTuples.remove(drainedKey);
                enrichTuple(drainedKey, pt.tuple, pt.timestamp, out);
            }
        }
        prefetcher.drainWrites();
    }

    private void enrichTuple(long key, NexmarkEvent value, long currentTime, Collector<Tuple2<NexmarkEvent, String>> out){
        List<String> stateForThisKeyList = prefetcher.readOrCreateEntryInCache(key, currentTime);
        if (stateForThisKeyList == null) {
            throw new RuntimeException("Problem. Unassigned value");
        }
        String stateForThisKey = stateForThisKeyList.get(0);

        out.collect(new Tuple2<>(value, stateForThisKey));

        latencyTracker.updateLatency(1, value.processingTimeMilliSecondsStart, currentTime,
                getRuntimeContext().getTaskInfo().getIndexOfThisSubtask(), prefetcher.getCacheSize(), prefetcher.getMaxCacheSize());

    }

}
