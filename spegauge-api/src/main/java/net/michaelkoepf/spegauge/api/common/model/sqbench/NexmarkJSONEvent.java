package net.michaelkoepf.spegauge.api.common.model.sqbench;

import java.io.Serializable;

public class NexmarkJSONEvent implements Serializable {
    public NexmarkEvent.Type type;

    public long eventTimeStampMilliSecondsSinceEpoch;

    public long processingTimeMilliSecondsStart;

    private long PK;

    private long FK;

    public String jsonString;

    public NexmarkJSONEvent(NexmarkEvent.Type type, long eventTimeStampMilliSecondsSinceEpoch, String jsonString) {
        this.type = type;
        this.eventTimeStampMilliSecondsSinceEpoch = eventTimeStampMilliSecondsSinceEpoch;
        this.jsonString = jsonString;
        this.processingTimeMilliSecondsStart = System.currentTimeMillis();
    }

    public NexmarkJSONEvent(NexmarkEvent.Type type, long eventTimeStampMilliSecondsSinceEpoch, String jsonString, long PK) {
        this.type = type;
        this.eventTimeStampMilliSecondsSinceEpoch = eventTimeStampMilliSecondsSinceEpoch;
        this.jsonString = jsonString;
        this.processingTimeMilliSecondsStart = System.currentTimeMillis();
        this.PK = PK;
    }

    public NexmarkJSONEvent(NexmarkEvent.Type type, long eventTimeStampMilliSecondsSinceEpoch, String jsonString, long PK, long FK) {
        this.type = type;
        this.eventTimeStampMilliSecondsSinceEpoch = eventTimeStampMilliSecondsSinceEpoch;
        this.jsonString = jsonString;
        this.processingTimeMilliSecondsStart = System.currentTimeMillis();
        this.PK = PK;
        this.FK = FK;
    }

    public NexmarkJSONEvent(NexmarkEvent.Type type, long PK, long eventTimeStampMilliSecondsSinceEpoch, String jsonString) {
        this.type = type;
        this.eventTimeStampMilliSecondsSinceEpoch = eventTimeStampMilliSecondsSinceEpoch;
        this.jsonString = jsonString;
        this.processingTimeMilliSecondsStart = System.currentTimeMillis();
        this.PK = PK;
    }

    public long getPK() {
        return PK;
    }

    public long getFK() {
        if (type == NexmarkEvent.Type.PERSON || type == NexmarkEvent.Type.AUCTION) {
            throw new RuntimeException("Attempted to read FK for Auction or Person event (they do not have an FK).");
        }
        return FK;
    }
}
