package net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.topN;

import net.michaelkoepf.spegauge.api.common.model.sqbench.EventWithTimestamp;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.LatencyTracker;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

// for Q18: K is Tuple2<Long, Long> (bidder, auction), T is Bid
// for Q19: K is Long (auction), T is Bid
public class TopN<K, T extends EventWithTimestamp> extends KeyedProcessFunction<K, T, List<T>> {
    protected final int n;

    protected transient ValueState<List<T>> topNElements;

    protected transient LatencyTracker latencyTracker;

    protected EventTypeHelper<K, T> eventTypeHelper;

    private boolean q18;

    public TopN(int n, boolean q18) {
        this.n = n;
        this.q18 = q18;
    }

    @Override
    public void open(OpenContext parameters) throws Exception {
        if (q18) {
            eventTypeHelper = (EventTypeHelper<K, T>) new TopNNexmarkQ18EventHelper();
        } else {
            eventTypeHelper = (EventTypeHelper<K, T>) new TopNNexmarkQ19EventHelper();
        }
        latencyTracker = new LatencyTracker(200000);
        String stateName = "topNElements";
        topNElements = getRuntimeContext().getState(eventTypeHelper.getStateDescriptor(stateName));
        System.out.println("streamIndex,time,taskIndex,average,p50,p60,p70,p80,p90,p95,p99,p999,cacheSize,maxCacheSize");
    }

    @Override
    public void processElement(T value, Context ctx, Collector<List<T>> out) throws Exception {
        long startTime = value.processingTimeMilliSecondsStart;

        List<T> stateForThisKey = topNElements.value();
        if (stateForThisKey == null){
            stateForThisKey = new ArrayList<>();
        }

        int insertPos = 0;
        for (T element : stateForThisKey) {
            if (compareElements(value, element)){ // new record should be before current
                break;

            }
            insertPos++;
        }
        stateForThisKey.add(insertPos, value);
        if (stateForThisKey.size() > n){
            stateForThisKey.remove(stateForThisKey.size() - 1);
        }

        topNElements.update(stateForThisKey);

        out.collect(stateForThisKey);

        latencyTracker.updateLatency(1, startTime, ctx.timestamp(), getRuntimeContext().getTaskInfo().getIndexOfThisSubtask());
    }

    protected boolean compareElements(T elem1, T elem2) {
        return eventTypeHelper.compareElements(elem1, elem2);
    }


}
