package net.michaelkoepf.spegauge.flink.queries.sqbench.metrics;

import lombok.Getter;
import lombok.Setter;
import net.michaelkoepf.spegauge.api.common.model.sqbench.EntityRecordFull;

@Getter
public class MeasuredEntity {

    private final EntityRecordFull entity;

    @Setter
    private long duration;

    public MeasuredEntity(EntityRecordFull entity) {
        this.entity = entity;
    }

    public String toString() {
        return Long.toString(duration);
    }
}
