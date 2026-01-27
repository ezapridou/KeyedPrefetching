package net.michaelkoepf.spegauge.flink.queries.sqbench.metrics;

import org.apache.flink.api.common.functions.MapFunction;

public class MeasureEvent implements MapFunction<TimestampedEntity, String> {
    private final String filename;

    public MeasureEvent(String filename) {
        this.filename = filename;
    }

    @Override
    public String map(TimestampedEntity value) throws Exception {
        return " " + (System.nanoTime() - value.getTimestamp());
    }
}
