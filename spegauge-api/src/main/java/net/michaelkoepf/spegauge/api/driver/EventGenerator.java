package net.michaelkoepf.spegauge.api.driver;

import java.util.Iterator;

/** An event generator that provides a sequence of events. */
public interface EventGenerator extends Iterator<Event> {

    /**
     * Indicates if the event generator is rate limited.
     *
     * @return Returns true if the generator is rate limited (otherwise false).
     */
    boolean isRateLimited();

    /**
     * Returns the rate limit information.
     *
     * @return Returns tbe event generator's rate limit information if it is rate limited (otherwise null).
     */
    RateLimitInformation getRateLimitInformation();

}
