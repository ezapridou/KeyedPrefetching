package net.michaelkoepf.spegauge.generators.nexmark;

import net.michaelkoepf.spegauge.api.common.model.nexmark.Event;

import java.util.Objects;

public class NEXMarkEvent implements net.michaelkoepf.spegauge.api.driver.Event {

    /** When, in wallclock time, should this event be emitted? */
    public final long wallclockTimestamp;

    /** When, in event time, should this event be considered to have occurred? */
    public final long eventTimestamp;

    /** The event itself. */
    public final Event event;

    /** The minimum of this and all future event timestamps. */
    public final long watermark;

    public NEXMarkEvent(long wallclockTimestamp, long eventTimestamp, Event event, long watermark) {
        this.wallclockTimestamp = wallclockTimestamp;
        this.eventTimestamp = eventTimestamp;
        this.event = event;
        this.watermark = watermark;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        NEXMarkEvent nextEvent = (NEXMarkEvent) o;

        return (wallclockTimestamp == nextEvent.wallclockTimestamp
                && eventTimestamp == nextEvent.eventTimestamp
                && watermark == nextEvent.watermark
                && event.equals(nextEvent.event));
    }

    @Override
    public int hashCode() {
        return Objects.hash(wallclockTimestamp, eventTimestamp, watermark, event);
    }

//    @Override
//    public int compareTo(NEXMarkEvent other) {
//        int i = Long.compare(wallclockTimestamp, other.wallclockTimestamp);
//        if (i != 0) {
//            return i;
//        }
//        return Integer.compare(event.hashCode(), other.event.hashCode());
//    }


    @Override
    public String serializeToString() {
        return this.event.toStringCSV();
    }

    @Override
    public byte[] serializeToBytes() {
        return this.event.toStringCSV().getBytes();
    }

    @Override
    public long getEventId() {
        throw new UnsupportedOperationException("getEventId");
    }
}
