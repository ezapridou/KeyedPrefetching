package net.michaelkoepf.spegauge.flink.sdk.source.impl;

import net.michaelkoepf.spegauge.api.common.model.sqbench.EntityRecordLimitedAttrs;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.concurrent.atomic.AtomicLong;

// Used for join and aggregation queries
public class SQBenchParallelTCPSourceNoExtraAttrs extends SQBenchParallelTCPSource<EntityRecordLimitedAttrs> {
    private AtomicLong currentEventTimeStampMilliSecondsSinceEpoch = new AtomicLong(0L);

    // overriding static method
    protected static EntityRecordLimitedAttrs attributesToEntityRecord(boolean isOfTypeA, String[] attrs) {
        int joinKey = isOfTypeA ? (int)Long.parseLong(attrs[4]) : (int)Long.parseLong(attrs[5]);
        return new EntityRecordLimitedAttrs(isOfTypeA, joinKey, attrs[10].isEmpty() ? null : Integer.parseInt(attrs[10]),
                Integer.parseInt(attrs[15]), Long.parseLong(attrs[2]));
    }

    protected EntityRecordLimitedAttrs handleEvent(String record) {
        String[] attrs = record.split("\t", -1);
        EntityRecordLimitedAttrs entity;

        if (attrs[0].equals("A") || attrs[0].equals("B")) {
            if (attrs.length != 16) {
                throw new RuntimeException("Entities must have length 16. Got " + attrs.length + " instead");
            }
            switch (attrs[0]) {
                case "A":
                    entity = attributesToEntityRecord(true, attrs);
                    break;
                case "B":
                    entity = attributesToEntityRecord(false, attrs);
                    break;
                default:
                    throw new IllegalArgumentException("Type must be A or B");
            }
        }
            else {
                throw new IllegalArgumentException("Type must be A or B");
        }
        currentEventId.set(Long.parseLong(attrs[1]));
        currentEventTimeStampMilliSecondsSinceEpoch.set(Long.parseLong(attrs[2]));

        return entity;
    }
}
