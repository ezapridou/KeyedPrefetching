package net.michaelkoepf.spegauge.api.common.model.sqbench;

import java.io.Serializable;

public class NexmarkJSONEvent implements Serializable {
    public NexmarkEvent.Type type;

    public long eventTimeStampMilliSecondsSinceEpoch;

    public long processingTimeMilliSecondsStart;

    public String jsonString;

    public NexmarkJSONEvent(NexmarkEvent.Type type, long eventTimeStampMilliSecondsSinceEpoch, String jsonString) {
        this.type = type;
        this.eventTimeStampMilliSecondsSinceEpoch = eventTimeStampMilliSecondsSinceEpoch;
        this.jsonString = jsonString;
        this.processingTimeMilliSecondsStart = System.currentTimeMillis();
    }

}
