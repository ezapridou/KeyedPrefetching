package net.michaelkoepf.spegauge.api.driver;

import lombok.Getter;

@Getter
public final class RateLimitInformation {

    private final long eventsPerSecond;
    private final int batchSize;
    private final long sleepTimeMS;
    private final long sleepTimeNS;

    public RateLimitInformation(long eventsPerSecond, int numGenerators) {
        this.eventsPerSecond = eventsPerSecond;
        this.batchSize = (int) Math.ceil((double) this.eventsPerSecond/numGenerators);

        // Calculate sleep time
        long t = (long) (1.0e6 * this.batchSize / this.eventsPerSecond * numGenerators);
        sleepTimeMS = t / 1_000;
        sleepTimeNS = t % 1_000;
    }

}
