package net.michaelkoepf.spegauge.flink.sdk.source.impl;

import net.michaelkoepf.spegauge.api.common.model.sqbench.NexmarkEvent;
import net.michaelkoepf.spegauge.api.common.model.sqbench.NexmarkJSONEvent;
import net.michaelkoepf.spegauge.api.common.model.sqbench.YSBJSONEvent;
import net.michaelkoepf.spegauge.api.common.model.sqbench.YSB_Event;

public class SQBenchParallelTCPSourceYSBJSON extends SQBenchParallelTCPSource<YSBJSONEvent> {
    protected YSBJSONEvent handleEvent(String record) {
        String[] attrs = record.split("\t", -1);
        YSBJSONEvent entity;

        switch (attrs[0]) {
            case "JSONA":
                if (attrs.length != 3) {
                    throw new RuntimeException("JSON entities must have length 3. Got " + attrs.length + " instead");
                }
                entity = new YSBJSONEvent(Long.parseLong(attrs[1]), attrs[2]);
                break;
            default:
                throw new RuntimeException("Invalid event type " + attrs[0]);
                // TODO: extend for further entities, if necessary
        }

        currentEventId.set(Long.parseLong(attrs[1]));

        return entity;
    }

}

