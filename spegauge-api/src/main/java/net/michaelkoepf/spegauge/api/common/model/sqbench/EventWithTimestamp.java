package net.michaelkoepf.spegauge.api.common.model.sqbench;

import java.io.Serializable;

public class EventWithTimestamp implements Serializable {
    public long eventTimeStampMilliSecondsSinceEpoch;

    public long processingTimeMilliSecondsStart;

    public EventWithTimestamp() {

    }

    public EventWithTimestamp(long eventTimeStampMilliSecondsSinceEpoch, long processingTimeMilliSecondsStart) {
        this.eventTimeStampMilliSecondsSinceEpoch = eventTimeStampMilliSecondsSinceEpoch;
        this.processingTimeMilliSecondsStart = processingTimeMilliSecondsStart;
    }
}
