package net.michaelkoepf.spegauge.generators.sqbench.common;

import org.apache.commons.rng.UniformRandomProvider;

/**
 * A discrete distribution that implements hot/cold selection.
 * With probability (hotRatio - 1) / hotRatio, returns a hot index,
 * and with probability 1 / hotRatio, returns a non-hot.
 */

public class HotColdDistributionSamplerPerson {

    private final int hotRatio = 4;
    private final long hotIdsRatio;

    private static long numActivePeople; // if this is bigger than the number of Person events in a window, then some auctions will produce no joins with person events.

    private static final int PERSON_ID_LEAD = 10;

    /** Proportions of people/auctions/bids to synthesize. */
    public static long PERSON_PROPORTION;// = 1;

    public static long AUCTION_PROPORTION;// = 3;
    private static long BID_PROPORTION;// = 46;
    public static long PROPORTION_DENOMINATOR;

    public static final long FIRST_PERSON_ID = 1000L;

    public HotColdDistributionSamplerPerson(SQBenchConfiguration.Scenario scenario) {
        // set the number of active people based on the scenario
        //long windowSizeSec = 3600; // 1 hour

        long totalEventsPer2Hours = scenario.numEventsPerSecond * 3600 * 2;
        this.AUCTION_PROPORTION = scenario.proportionEntityA;
        this.BID_PROPORTION = scenario.proportionEntityB;
        this.PERSON_PROPORTION = scenario.proportionEntityC;
        PROPORTION_DENOMINATOR = PERSON_PROPORTION + AUCTION_PROPORTION + BID_PROPORTION;

        long totalPeoplePer2Hours = (totalEventsPer2Hours * PERSON_PROPORTION) / PROPORTION_DENOMINATOR;
        numActivePeople = totalPeoplePer2Hours;

        hotIdsRatio = (scenario.numEventsPerSecond * PERSON_PROPORTION) / PROPORTION_DENOMINATOR;
    }

    // used to generate auction.seller
    public long sample(long eventId, UniformRandomProvider random) {
        long value;
        if (random.nextInt(hotRatio) > 0) {
            value = (lastBase0PersonId(eventId) / hotIdsRatio) * hotIdsRatio;
        } else {
            value = nextBase0PersonId(eventId, random);
        }
        return value + FIRST_PERSON_ID;
    }

    /** Return a random person id (base 0). */
    private static long nextBase0PersonId(long eventId, UniformRandomProvider random) {
        // Choose a random person from any of the 'active' people, plus a few 'leads'.
        // By limiting to 'active' we ensure the density of bids or auctions per person
        // does not decrease over time for long-running jobs.
        // By choosing a person id ahead of the last valid person id we will make
        // newPerson and newAuction events appear to have been swapped in time.
        long numPeople = lastBase0PersonId(eventId) + 1;
        long activePeople = Math.min(numPeople, numActivePeople);
        long n = nextLong(random, activePeople + PERSON_ID_LEAD);
        return numPeople - activePeople + n;
    }

    /**
     * Return the last valid person id (ignoring FIRST_PERSON_ID). Will be the current person id if
     * due to generate a person.
     */
    public static long lastBase0PersonId(long eventId) {
        long epoch = eventId / PROPORTION_DENOMINATOR;
        long offset = eventId % PROPORTION_DENOMINATOR;
        if (offset >= PERSON_PROPORTION) {
            // About to generate an auction or bid.
            // Go back to the last person generated in this epoch.
            offset = PERSON_PROPORTION - 1;
        }
        // About to generate a person.
        return epoch * PERSON_PROPORTION + offset;
    }

    /** Return a random long from {@code [0, n)}. */
    private static long nextLong(UniformRandomProvider random, long n) {
        if (n < Integer.MAX_VALUE) {
            return random.nextInt((int) n);
        } else {
            // WARNING: Very skewed distribution! Bad!
            return Math.abs(random.nextLong() % n);
        }
    }
}
