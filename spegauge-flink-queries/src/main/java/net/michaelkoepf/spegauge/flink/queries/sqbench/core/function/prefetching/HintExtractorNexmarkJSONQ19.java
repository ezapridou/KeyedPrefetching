package net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.prefetching;

import net.michaelkoepf.spegauge.api.common.model.sqbench.NexmarkEvent;
import net.michaelkoepf.spegauge.api.common.model.sqbench.NexmarkJSONEvent;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.LookaheadOperator;
import org.apache.flink.util.Collector;

public class HintExtractorNexmarkJSONQ19 extends HintExtractorNexmarkJSON<Long, Tuple2<Long, Long>> implements LookaheadOperator {
    private final int candidateLookaheadID;

    private boolean active = true;
    public HintExtractorNexmarkJSONQ19(boolean PK, boolean hintsForAuction, int candidateLookaheadID) {
        super(PK, hintsForAuction);
        this.candidateLookaheadID = candidateLookaheadID;
    }

    @Override
    public void processElement(NexmarkJSONEvent value, Context ctx, Collector<Tuple2<Long, Long>> out) throws Exception {
        if (!active) {
            return;
        }
        if (value.type == NexmarkEvent.Type.MARKER) {
            // pass markers
            // assign as key the lookahead id * (-1)
            out.collect(new Tuple2<>((-1l) * candidateLookaheadID, value.processingTimeMilliSecondsStart));
        }
        else if ((value.type == NexmarkEvent.Type.AUCTION) == hintsForAuction) {
            long key = PK ? value.getPK() : value.getFK(); //extractFieldFromJSON(value.jsonString, fieldName);
            // non-hot elements filter
            if (!cms.update(key)) {
                out.collect(new Tuple2<>(key, value.eventTimeStampMilliSecondsSinceEpoch));
            }
        }
    }

    // exact isHot
    // used only to debug and compare
    @Override
    public boolean isHot(Long key) {
        // key % hot_seller_ratio != first_person_id
        long FIRST_AUCTION_ID = 1000l;
        //long hotIdsRatio = 100;
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

    @Override
    public void setState(boolean active) {
        this.active = active;
    }
}
