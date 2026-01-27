package net.michaelkoepf.spegauge.generators.sqbench.common;

import org.apache.commons.rng.UniformRandomProvider;

/**
 * A discrete distribution that implements hot/cold selection.
 * With probability (hotRatio - 1) / hotRatio, returns a hot index,
 * and with probability 1 / hotRatio, returns a non-hot.
 */

public class HotColdDistributionSamplerAuctionID {

    private final int hotAuctionRatio = 2; //4; // default 2
    private final long hotIdsRatio;// = 5000; // increase for higher skewness default: 100

    private static long numInFlightAuctions; //100; // if this is bigger than the number of Auction events in a window, then some auctions will produce no joins with bid events.

    private static final int AUCTION_ID_LEAD = 10;

    /** Proportions of people/auctions/bids to synthesize. */
    public static long PERSON_PROPORTION;// = 1;

    public static long AUCTION_PROPORTION;// = 3;
    private static long BID_PROPORTION;// = 46;
    public static long PROPORTION_DENOMINATOR;

    public static final long FIRST_AUCTION_ID = 1000L;

    public HotColdDistributionSamplerAuctionID(SQBenchConfiguration.Scenario scenario) {
        // set the number of in-flight auctions based on the scenario
        // an auction stays "in-flight" for windowSizeSec seconds
        // the hot auction changes every 1 sec

        //long windowSizeSec = 3600; // 1 hour

        long totalEventsPer2Hours = scenario.numEventsPerSecond * 3600 *2;
        this.AUCTION_PROPORTION = scenario.proportionEntityA;
        this.BID_PROPORTION = scenario.proportionEntityB;
        this.PERSON_PROPORTION = scenario.proportionEntityC;
        PROPORTION_DENOMINATOR = PERSON_PROPORTION + AUCTION_PROPORTION + BID_PROPORTION;

        long totalAuctionsPer2Hours = (totalEventsPer2Hours * AUCTION_PROPORTION) / PROPORTION_DENOMINATOR;
        numInFlightAuctions = totalAuctionsPer2Hours;

        hotIdsRatio = (scenario.numEventsPerSecond * AUCTION_PROPORTION) / PROPORTION_DENOMINATOR;
    }

    // used to generate bid.auction
    public long sample(long eventId, UniformRandomProvider random) {
        long auction;
        // Here P(bid will be for a hot auction) = 1 - 1/hotAuctionRatio.
        if (random.nextInt(hotAuctionRatio) > 0) {
            // Choose the first auction in the batch of last HOT_AUCTION_RATIO auctions.
            auction = (lastBase0AuctionId(eventId) / hotIdsRatio) * hotIdsRatio;
        } else {
            auction = nextBase0AuctionId(eventId, random);
        }
        return auction + FIRST_AUCTION_ID;
    }

    /** Return a random auction id (base 0). */
    public static long nextBase0AuctionId(long eventId, UniformRandomProvider random) {
        // Choose a random auction for any of those which are likely to still be in flight, plus a few 'leads'.
        // Note that ideally we'd track non-expired auctions exactly, but that state is difficult to split.
        long maxAuction = lastBase0AuctionId(eventId); // 60_000_000 - 10;
        long minAuction = Math.max(maxAuction - numInFlightAuctions, 0); // 0
        return minAuction + nextLong(random, maxAuction - minAuction + 1 + AUCTION_ID_LEAD);
    }

    /**
     * Return the last valid auction id (ignoring FIRST_AUCTION_ID). Will be the current auction id if
     * due to generate an auction.
     */
    public static long lastBase0AuctionId(long eventId) {
        long epoch = eventId / PROPORTION_DENOMINATOR;
        long offset = eventId % PROPORTION_DENOMINATOR;
        if (offset < PERSON_PROPORTION) {
            // About to generate a person.
            // Go back to the last auction in the last epoch.
            epoch--;
            offset = AUCTION_PROPORTION - 1;
        } else if (offset >= PERSON_PROPORTION + AUCTION_PROPORTION) {
            // About to generate a bid.
            // Go back to the last auction generated in this epoch.
            offset = AUCTION_PROPORTION - 1;
        } else {
            // About to generate an auction.
            offset -= PERSON_PROPORTION;
        }
        return epoch * AUCTION_PROPORTION + offset;
    }

    /** Return a random long from {@code [0, n)}. */
    public static long nextLong(UniformRandomProvider random, long n) {
        if (n < Integer.MAX_VALUE) {
            return random.nextInt((int) n);
        } else {
            // WARNING: Very skewed distribution! Bad!
            return Math.abs(random.nextLong() % n);
        }
    }
}
