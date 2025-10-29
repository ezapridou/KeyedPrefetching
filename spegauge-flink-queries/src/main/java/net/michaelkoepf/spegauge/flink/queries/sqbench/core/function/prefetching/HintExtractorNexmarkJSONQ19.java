package net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.prefetching;

import net.michaelkoepf.spegauge.api.common.model.sqbench.NexmarkEvent;
import net.michaelkoepf.spegauge.api.common.model.sqbench.NexmarkJSONEvent;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class HintExtractorNexmarkJSONQ19 extends HintExtractorNexmarkJSON<Long, Tuple2<Long, Long>>{
    public HintExtractorNexmarkJSONQ19(String fieldName, boolean hintsForAuction) {
        super(fieldName, hintsForAuction);
    }

    @Override
    public void processElement(NexmarkJSONEvent value, Context ctx, Collector<Tuple2<Long, Long>> out) throws Exception {
        if ((value.type == NexmarkEvent.Type.AUCTION) == hintsForAuction) {
            long key = extractFieldFromJSON(value.jsonString, fieldName);
            // non-hot elements filter
            if (!isHot(key)) {
                out.collect(new Tuple2<>(key, value.eventTimeStampMilliSecondsSinceEpoch));
            }
        }

    }

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
}
