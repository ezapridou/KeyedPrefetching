package net.michaelkoepf.spegauge.flink.sdk.source.impl;

import net.michaelkoepf.spegauge.api.common.model.nexmark.Auction;
import net.michaelkoepf.spegauge.api.common.model.nexmark.Bid;
import net.michaelkoepf.spegauge.api.common.model.nexmark.Event;
import net.michaelkoepf.spegauge.api.common.model.nexmark.Person;
import net.michaelkoepf.spegauge.flink.sdk.source.SocketReader;

public class NEXMarkParallelTCPSource extends SocketReader<Event> {
    private long eventCount = 0;

    @Override
    public Event getEvent(String record) {
        String[] attrs = record.split(",");
        Event event;

        switch (attrs[0]) {
            case "0":  // PERSON
                if (attrs.length != 9) {
                    throw new RuntimeException("Attribute length is not correct for a Person event");
                }
                event = new Event(new Person(attrs));
                break;
            case "1":  // AUCTION
                if (attrs.length != 12) {
                    throw new RuntimeException("Attribute length is not correct for an Auction event");
                }

                event = new Event(new Auction(attrs));
                break;
            case "2":  // BID
                if (attrs.length != 6) {
                    throw new RuntimeException("Attribute length is not correct for a Bid event");
                }
                event = new Event(new Bid(attrs));
                break;
            default:
                throw new RuntimeException("Invalid event type " + attrs[0]);
        }

        eventCount++;

        return event;
    }

    @Override
    public Event getMarker(long eventCount){
        throw new UnsupportedOperationException("Markers not supported for Nexmark.");
    }
}
