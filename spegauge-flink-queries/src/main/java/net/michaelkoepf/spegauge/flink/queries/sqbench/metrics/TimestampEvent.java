package net.michaelkoepf.spegauge.flink.queries.sqbench.metrics;

import net.michaelkoepf.spegauge.api.common.model.sqbench.EntityRecordFull;
import org.apache.flink.api.common.functions.RichMapFunction;

public class TimestampEvent extends RichMapFunction<EntityRecordFull, TimestampedEntity> {
    private static final long serialVersionUID = 1L;

    @Override
    public TimestampedEntity map(EntityRecordFull value) throws Exception {
        return new TimestampedEntity(value, System.nanoTime());
    }
}
