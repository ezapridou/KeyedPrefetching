package net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.sideinputjoin;

import net.michaelkoepf.spegauge.api.QueryConfig;
import net.michaelkoepf.spegauge.api.common.model.sqbench.NexmarkEvent;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.AsyncBaseline;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.LatencyTracker;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.cache.CacheClock;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.cache.CacheLRU;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.cache.OperatorWithCacheHelper;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.sidehandling.StreamSide;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.internal.KeyAccessibleState;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

// for Q13
public class JoinSideInputAsync extends KeyedProcessFunction<Long, NexmarkEvent, Tuple2<NexmarkEvent, String>> {
    protected transient ValueState<List<String>> staticTable;
    protected transient KeyAccessibleState<Long, List<String>> keyAccessibleState;

    protected transient LatencyTracker latencyTracker;

    protected KeyGroupRange keyGroupRange;
    protected int maxParallelism = 192;

    private final int cacheSize = 93750; // (20gb/24 task slots/8000 value size)*0.9

    private AsyncBaseline<Long, String> asyncStateClient;

    private StreamSide side = StreamSide.FIRST; // only one input, so always FIRST

    private long windowSize = Long.MAX_VALUE, windowSlide = 0; // no expiration of entries
    private final int cacheType = 2; // 0 = TopKeys, 1 = LRU, 2 = Clock

    private Map<Long, PendingTuple> pendingTuples = new HashMap<>();
    public static class PendingTuple {
        public final long key;
        public final NexmarkEvent tuple;
        public final long timestamp;

        public PendingTuple(long key, NexmarkEvent tuple, long timestamp) {
            this.key = key;
            this.tuple = tuple;
            this.timestamp = timestamp;
        }
    }


    public JoinSideInputAsync() {
        super();
    }

    @Override
    public void open(OpenContext parameters) throws Exception {
        latencyTracker = new LatencyTracker(200000);
        String stateName = "staticTable";
        staticTable = getRuntimeContext().getState(new ValueStateDescriptor<List<String>>(stateName, Types.LIST(Types.STRING)));
        System.out.println("streamIndex,time,taskIndex,average,p50,p60,p70,p80,p90,p95,p99,p999,cacheSize,maxCacheSize");
        keyAccessibleState = OperatorWithCacheHelper.getKeyAccessibleValueState(staticTable, stateName);

        int operatorIndex = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        int parallelism = QueryConfig.PARALLELISM;
        int maxParallelism = QueryConfig.MAX_PARALLELISM;
        int start = ((operatorIndex * maxParallelism + parallelism - 1) / parallelism);
        int end = ((operatorIndex + 1) * maxParallelism - 1) / parallelism;
        keyGroupRange = new KeyGroupRange(start, end);

        JoinSideInput.fillStaticTable(keyGroupRange, maxParallelism, keyAccessibleState, operatorIndex, parallelism);

        var cache = (cacheType== 1) ? new CacheLRU<Long, String>(cacheSize, true) :
                new CacheClock<Long, String>(cacheSize, true);

        asyncStateClient = new AsyncBaseline<>(cache, false);
        asyncStateClient.wire(side, keyAccessibleState);
    }

    @Override
    public void close() {
        asyncStateClient.shutdown();
    }

    @Override
    public void processElement(NexmarkEvent value, Context ctx, Collector<Tuple2<NexmarkEvent, String>> out) throws Exception {
        long key = ctx.getCurrentKey();
        long currentTime = ctx.timestamp();

        while(asyncStateClient.hasCompletedFetchesForPending()){
            long drainedKey = asyncStateClient.drainOnePendingResult(windowSize, windowSlide);
            PendingTuple pt = pendingTuples.remove(drainedKey);
            enrichTuple(drainedKey, pt.tuple, pt.timestamp, out);
        }

        // ensure state is in cache now
        boolean hot = asyncStateClient.fetchKeyAsync(side, key, key, currentTime, true, windowSize, windowSlide);

        if (hot) {
            enrichTuple(key, value, currentTime, out);
        }
        else {
            // remember this tuple for later processing
            pendingTuples.put(key, new PendingTuple(key, value, currentTime));
        }
    }

    private void enrichTuple(long key, NexmarkEvent value, long currentTime, Collector<Tuple2<NexmarkEvent, String>> out){
        List<String> stateForThisKeyList = asyncStateClient.readOrCreateEntryInCache(key, currentTime);
        if (stateForThisKeyList == null) {
            throw new RuntimeException("Problem. Unassigned value");
        }
        String stateForThisKey = stateForThisKeyList.get(0);

        out.collect(new Tuple2<>(value, stateForThisKey));

        latencyTracker.updateLatency(1, value.processingTimeMilliSecondsStart, currentTime,
                getRuntimeContext().getTaskInfo().getIndexOfThisSubtask(), asyncStateClient.getCacheSize(),
                asyncStateClient.getMaxCacheSize());

    }
}
