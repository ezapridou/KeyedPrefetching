package net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.prefetching;

import net.michaelkoepf.spegauge.api.common.model.sqbench.NexmarkEvent;


public class HintExtractorNexmarkQ20 extends HintExtractorNexmark{
    public HintExtractorNexmarkQ20(boolean hintsForAuction) {
        super(hintsForAuction);
    }

    @Override
    public long getKey(NexmarkEvent event) {
        return event.newAuction.id;
    }

    // exact isHot
    // used only to debug and compare
    @Override
    public boolean isHot(long key) {
        long FIRST_AUCTION_ID = 1000l;
        long hotIdsRatio = (150_000 * 3) / 49;
        if (key < FIRST_AUCTION_ID) return false;
        if ((key - FIRST_AUCTION_ID) % hotIdsRatio == 0){ // hot auction
            if (key > lastHotAuction) {
                lastHotAuction = key;
                return false;
            }
            return true;
        }
        return false;
    }
}
