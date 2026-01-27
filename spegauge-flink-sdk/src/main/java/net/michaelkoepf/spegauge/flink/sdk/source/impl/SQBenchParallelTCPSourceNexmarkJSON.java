package net.michaelkoepf.spegauge.flink.sdk.source.impl;

import net.michaelkoepf.spegauge.api.common.model.sqbench.NexmarkEvent;
import net.michaelkoepf.spegauge.api.common.model.sqbench.NexmarkJSONEvent;

public class SQBenchParallelTCPSourceNexmarkJSON extends SQBenchParallelTCPSource<NexmarkJSONEvent> {
    private final long MARKER_INTERVAL = 20000L;
    private final boolean ENABLE_MARKERS;

    public SQBenchParallelTCPSourceNexmarkJSON(boolean markersOn) {
        super();
        ENABLE_MARKERS = markersOn;
    }

    protected NexmarkJSONEvent handleEvent(String record) {
        String[] attrs = record.split("\t", -1);
        NexmarkJSONEvent entity;

        switch (attrs[0]) {
            case "JSONA":
                if (attrs.length != 4) {
                    throw new RuntimeException("JSON entities must have length 4. Got " + attrs.length + " instead");
                }
                entity = new NexmarkJSONEvent(NexmarkEvent.Type.AUCTION, Long.parseLong(attrs[1]), attrs[2], Long.parseLong(attrs[3]));
                break;
            case "JSONB":
                if (attrs.length != 5) {
                    throw new RuntimeException("JSON entities must have length 5. Got " + attrs.length + " instead");
                }
                entity = new NexmarkJSONEvent(NexmarkEvent.Type.BID, Long.parseLong(attrs[1]), attrs[2], Long.parseLong(attrs[3]), Long.parseLong(attrs[4]));
                break;
            case "JSONC":
                if (attrs.length != 4) {
                    throw new RuntimeException("JSON entities must have length 4. Got " + attrs.length + " instead");
                }
                entity = new NexmarkJSONEvent(NexmarkEvent.Type.PERSON, Long.parseLong(attrs[3]), Long.parseLong(attrs[1]), attrs[2]);
                break;
            default:
                throw new RuntimeException("Invalid event type " + attrs[0]);
                // TODO: extend for further entities, if necessary
        }

        currentEventId.set(Long.parseLong(attrs[1]));

        return entity;
    }

    public NexmarkJSONEvent getMarker(long eventCount) {
        if (ENABLE_MARKERS && eventCount > 3000000 && eventCount % MARKER_INTERVAL == 0) {
            return new NexmarkJSONEvent(NexmarkEvent.Type.MARKER, 0, "");
        }
        return null;
    }

}
