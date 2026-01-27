package net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.prefetching;

import net.michaelkoepf.spegauge.api.common.model.sqbench.NexmarkEvent;
import net.michaelkoepf.spegauge.api.common.model.sqbench.NexmarkJSONEvent;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public class HintExtractorNexmarkJSONQ20 extends HintExtractorNexmarkJSON<Long, Tuple3<Long, Long, Boolean>>{
    public HintExtractorNexmarkJSONQ20(boolean PK, boolean hintsForAuction) {
        super(PK, hintsForAuction);
    }

    @Override
    public void processElement(NexmarkJSONEvent value, Context ctx, Collector<Tuple3<Long, Long, Boolean>> out) throws Exception {
        if ((value.type == NexmarkEvent.Type.AUCTION) == hintsForAuction) {
            long key = PK ? value.getPK() : value.getFK(); //extractFieldFromJSON(value.jsonString, fieldName);
            // non-hot elements filter
            if (!cms.update(key)) {
                out.collect(new Tuple3<>(key, value.eventTimeStampMilliSecondsSinceEpoch, hintsForAuction));
            }
        }

    }

    // exact isHot
    // used only to debug and compare
    @Override
    public boolean isHot(Long key) {
        // key % hot_seller_ratio != first_person_id
        long FIRST_AUCTION_ID = 1000l;
        //long hotIdsRatio = 8000;
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
