package net.michaelkoepf.spegauge.flink.queries.sqbench.metrics;

import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

public class ObservableWindow extends KeyedCoProcessFunction<Long, TimestampedEntity, TimestampedEntity, Long> {
    private final long windowSize;

    public ObservableWindow(long windowSize) {
        this.windowSize = 1000;  // currently only tumbling window
    }

    @Override
    public void processElement1(TimestampedEntity value, KeyedCoProcessFunction<Long, TimestampedEntity, TimestampedEntity, Long>.Context ctx, Collector<Long> out) throws Exception {
        // here, we can measure the cost of key by per tuple
        long eventTime = value.getEntity().eventTimeStampMilliSecondsSinceEpoch;
        long endOfWindow = (eventTime - (eventTime % windowSize) + windowSize - 1);
        // assign element to window
        ctx.timerService().registerEventTimeTimer(endOfWindow);
    }

    @Override
    public void processElement2(TimestampedEntity value, KeyedCoProcessFunction<Long, TimestampedEntity, TimestampedEntity, Long>.Context ctx, Collector<Long> out) throws Exception {
        // here, we can measure the cost of key by per tuple
        long eventTime = value.getEntity().eventTimeStampMilliSecondsSinceEpoch;
        long endOfWindow = (eventTime - (eventTime % windowSize) + windowSize - 1);
        // assign element to window
        ctx.timerService().registerEventTimeTimer(endOfWindow);
    }


    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Long> out) throws Exception {
        // TODO: how to measure the cost per tuple here? just divide total processing time by number of tuples?
        super.onTimer(timestamp, ctx, out);
        // since we have a keyed stream, we just need to create the crossproduct of event 1 and event 2
        // what are alternatives to this?
    }
}
