package net.michaelkoepf.spegauge.flink.queries.sqbench.metrics;

import net.michaelkoepf.spegauge.api.common.model.sqbench.EntityRecordFull;

public class TimestampedEntity {
    private final long timestamp;
    private final EntityRecordFull entity;

    public TimestampedEntity(EntityRecordFull entity, long timestamp) {
        this.entity = entity;
        this.timestamp = timestamp;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public EntityRecordFull getEntity() {
        return entity;
    }
}
