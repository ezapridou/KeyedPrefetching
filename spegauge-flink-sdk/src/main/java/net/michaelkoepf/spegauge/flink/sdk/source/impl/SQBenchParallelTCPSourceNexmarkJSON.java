package net.michaelkoepf.spegauge.flink.sdk.source.impl;

import net.michaelkoepf.spegauge.api.common.model.sqbench.NexmarkEvent;
import net.michaelkoepf.spegauge.api.common.model.sqbench.NexmarkJSONEvent;

public class SQBenchParallelTCPSourceNexmarkJSON extends SQBenchParallelTCPSource<NexmarkJSONEvent> {
    protected NexmarkJSONEvent handleEvent(String record) {
        String[] attrs = record.split("\t", -1);
        NexmarkJSONEvent entity;

        switch (attrs[0]) {
            case "JSONA":
                if (attrs.length != 3) {
                    throw new RuntimeException("JSON entities must have length 3. Got " + attrs.length + " instead");
                }
                entity = new NexmarkJSONEvent(NexmarkEvent.Type.AUCTION, Long.parseLong(attrs[1]), attrs[2]);
                break;
            case "JSONB":
                if (attrs.length != 3) {
                    throw new RuntimeException("JSON entities must have length 3. Got " + attrs.length + " instead");
                }
                entity = new NexmarkJSONEvent(NexmarkEvent.Type.BID, Long.parseLong(attrs[1]), attrs[2]);
                break;
            case "JSONC":
                if (attrs.length != 3) {
                    throw new RuntimeException("JSON entities must have length 3. Got " + attrs.length + " instead");
                }
                entity = new NexmarkJSONEvent(NexmarkEvent.Type.PERSON, Long.parseLong(attrs[1]), attrs[2]);
                break;
            default:
                throw new RuntimeException("Invalid event type " + attrs[0]);
                // TODO: extend for further entities, if necessary
        }

        currentEventId.set(Long.parseLong(attrs[1]));

        return entity;
    }

}
