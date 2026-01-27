package net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.sideinputjoin;

import net.michaelkoepf.spegauge.api.QueryConfig;
import net.michaelkoepf.spegauge.api.common.model.sqbench.NexmarkEvent;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.LatencyTracker;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.cache.CacheLRU;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.cache.OperatorWithCacheHelper;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.prefetching.Prefetcher;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.sidehandling.StreamSide;
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
import org.apache.flink.streaming.api.operators.StatefulOperatorWithPrefetching;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JoinSideInputPrefetching
        extends KeyedCoProcessFunction<Long, Tuple2<Long, Long>, NexmarkEvent, Tuple2<NexmarkEvent, String>>
        implements InputSelectable, StatefulOperatorWithPrefetching {
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

    // Lookahead selection mechanism
    Map<Long, Integer> numReceivedMarkerHints = new HashMap<>();
    int numExpectedMarkerHints;
    Map<Long, Map<Integer, Long>> arrivalTimesMarkerHints = new HashMap<>();

    Map<Long, Long> arrivalTimeOfMarker = new HashMap<>();

    long lastP999 = Long.MAX_VALUE;

    int currentLookaheadID = 1;

    public JoinSideInputPrefetching(int numExpectedMarkerHints) {
        this.numExpectedMarkerHints = numExpectedMarkerHints;
    }

    @Override
    public void open(OpenContext parameters) throws Exception {
        latencyTracker = new LatencyTracker(200000);
        String stateName = "staticTable";
        staticTable = getRuntimeContext().getState(new ValueStateDescriptor<List<String>>(stateName, Types.LIST(Types.STRING)));
        System.out.println("streamIndex,time,taskIndex,average,p50,p60,p70,p80,p90,p95,p99,p999");
        keyAccessibleState = OperatorWithCacheHelper.getKeyAccessibleValueState(staticTable, stateName);

        int operatorIndex = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        int parallelism = QueryConfig.PARALLELISM;
        int maxParallelism = QueryConfig.MAX_PARALLELISM;
        int start = ((operatorIndex * maxParallelism + parallelism - 1) / parallelism);
        int end = ((operatorIndex + 1) * maxParallelism - 1) / parallelism;
        keyGroupRange = new KeyGroupRange(start, end);

        JoinSideInput.fillStaticTable(keyGroupRange, maxParallelism, keyAccessibleState, operatorIndex, parallelism);

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

        if (key < 0L) {
            // marker event
            int candidateLookaheadID = (int)((-1) * key);
            long currentTime = System.currentTimeMillis();
            long startTimeOfMarker = hint.f1;
            numReceivedMarkerHints.put(startTimeOfMarker, numReceivedMarkerHints.getOrDefault(startTimeOfMarker, 0) + 1);
            arrivalTimesMarkerHints.putIfAbsent(startTimeOfMarker, new HashMap<>());
            arrivalTimesMarkerHints.get(startTimeOfMarker).put(candidateLookaheadID, currentTime);
            selectNewLookahead(startTimeOfMarker);
            //System.out.println("Hint of marker arrived at subtask " + getRuntimeContext().getTaskInfo().getIndexOfThisSubtask());
            return;
        }

        //lastHintTs = Math.max(lastHintTs, timestampWhenStateNeeded);

        prefetcher.fetchKeyAsync(side, key, key, timestampWhenStateNeeded, 1, false, windowSize, windowSlide);
    }

    @Override
    public void processElement2(NexmarkEvent value, Context ctx, Collector<Tuple2<NexmarkEvent, String>> out) {
        long key = ctx.getCurrentKey();
        long currentTime = ctx.timestamp();

        if (value.type == NexmarkEvent.Type.MARKER) {
            // marker event
            // the key here is the start time of the marker
            long currentTimeMillis = System.currentTimeMillis();
            arrivalTimeOfMarker.put(key, currentTimeMillis);
            selectNewLookahead(key);
            //System.out.println("Marker arrived at subtask " + getRuntimeContext().getTaskInfo().getIndexOfThisSubtask());
            return;
        }

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

        lastP999 = latencyTracker.updateLatencyAndReturnP999(1, value.processingTimeMilliSecondsStart, currentTime,
                getRuntimeContext().getTaskInfo().getIndexOfThisSubtask(), prefetcher.getCacheSize(), prefetcher.getMaxCacheSize());

    }

    private void selectNewLookahead(long startTimeOfMarker) {
        if (numReceivedMarkerHints.getOrDefault(startTimeOfMarker, 0) == numExpectedMarkerHints && arrivalTimeOfMarker.containsKey(startTimeOfMarker)) {
            // all hints for this lookahead have arrived
            Map<Integer, Long> hintArrivalTimes = arrivalTimesMarkerHints.get(startTimeOfMarker);
            long p999Delay = lastP999;
            long bestLookahead = currentLookaheadID;
            double timeliestDelay = Double.MAX_VALUE;
            for (Map.Entry<Integer, Long> entry : hintArrivalTimes.entrySet()) {
                int candidateLookaheadID = entry.getKey();
                long arrivalTime = entry.getValue();
                long delay = arrivalTimeOfMarker.get(startTimeOfMarker)  - arrivalTime;
                double metricToMinimize = delay - p999Delay * 1.1;
                if (metricToMinimize < timeliestDelay) {
                    timeliestDelay = metricToMinimize;
                    bestLookahead = candidateLookaheadID;
                }
            }
            if (bestLookahead != currentLookaheadID) {
                currentLookaheadID = (int) bestLookahead;
            }
            //System.out.println("New lookahead selected: " + currentLookaheadID + " at " + System.currentTimeMillis());
            numReceivedMarkerHints.remove(startTimeOfMarker);
            arrivalTimeOfMarker.remove(startTimeOfMarker);
        }
    }

    public int getSelectedLookaheadID() {
        return currentLookaheadID;
    }

}
