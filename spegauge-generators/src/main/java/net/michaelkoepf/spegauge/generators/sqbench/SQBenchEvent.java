package net.michaelkoepf.spegauge.generators.sqbench;

import net.michaelkoepf.spegauge.api.common.model.sqbench.EntityRecordFull;
import net.michaelkoepf.spegauge.api.driver.SheddableEvent;

public class SQBenchEvent implements SheddableEvent {

    public final long wallClockMilliSecondsSinceEpoch;
    public final long eventTimeMilliSecondsSinceEpoch;
    public final EntityRecordFull entity;
    private final SheddingInformation sheddingInformation;

    public SQBenchEvent(long wallClockMilliSecondsSinceEpoch, long eventTimeMilliSecondsSinceEpoch, EntityRecordFull entity) {
        this(wallClockMilliSecondsSinceEpoch, eventTimeMilliSecondsSinceEpoch, entity, null);
    }

    public SQBenchEvent(long wallClockMilliSecondsSinceEpoch, long eventTimeMilliSecondsSinceEpoch, EntityRecordFull entity, Long desiredThroughputPerSecond) {
        this.wallClockMilliSecondsSinceEpoch = wallClockMilliSecondsSinceEpoch;
        this.eventTimeMilliSecondsSinceEpoch = eventTimeMilliSecondsSinceEpoch;
        this.entity = entity;
        if (desiredThroughputPerSecond == null) {
            this.sheddingInformation = null;
        } else {
            this.sheddingInformation = new SheddingInformation(desiredThroughputPerSecond);
        }
    }

    @Override
    public String serializeToString() {
        return this.entity.toStringTSV();
    }

    @Override
    public long getEventId() {
        return this.entity.uniqueTupleId;
    }

    @Override
    public SheddingInformation getSheddingInformation() {
        return sheddingInformation;
    }
}
