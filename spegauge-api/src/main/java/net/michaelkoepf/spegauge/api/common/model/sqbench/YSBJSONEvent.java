package net.michaelkoepf.spegauge.api.common.model.sqbench;

import java.io.Serializable;

public class YSBJSONEvent implements Serializable {
    public long eventTimeStampMilliSecondsSinceEpoch;

    public long processingTimeMilliSecondsStart;

    public String jsonString;

    public YSBJSONEvent(long eventTimeStampMilliSecondsSinceEpoch, String jsonString) {
        this.eventTimeStampMilliSecondsSinceEpoch = eventTimeStampMilliSecondsSinceEpoch;
        this.jsonString = jsonString;
        this.processingTimeMilliSecondsStart = System.currentTimeMillis();
    }

}
