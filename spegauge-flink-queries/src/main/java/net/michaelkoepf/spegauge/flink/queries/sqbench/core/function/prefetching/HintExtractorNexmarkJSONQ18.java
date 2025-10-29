package net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.prefetching;

import net.michaelkoepf.spegauge.api.common.model.sqbench.NexmarkEvent;
import net.michaelkoepf.spegauge.api.common.model.sqbench.NexmarkJSONEvent;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class HintExtractorNexmarkJSONQ18 extends HintExtractorNexmarkJSON<Tuple2<Long, Long>, Tuple2<Tuple2<Long, Long>, Long>>{
    private final String fieldName2;
    private final long hotIdsPersonRatio = (80000 * 1) / 49;

    private Tuple2<Long, Long> lastHotKey = Tuple2.of(-1L, -1L);
    public HintExtractorNexmarkJSONQ18(String fieldName, String fieldName2, boolean hintsForAuction) {
        super(fieldName, hintsForAuction);
        this.fieldName2 = fieldName2;
    }

    @Override
    public void processElement(NexmarkJSONEvent value, Context ctx, Collector<Tuple2<Tuple2<Long, Long>, Long>> out) throws Exception {
        if ((value.type == NexmarkEvent.Type.AUCTION) == hintsForAuction) {
            long key1 = extractFieldFromJSON(value.jsonString, fieldName);
            long key2 = extractFieldFromJSON(value.jsonString, fieldName2);
            Tuple2<Long, Long> key = Tuple2.of(key1, key2);
            // non-hot elements filter
            if (!isHot(key)) {
                out.collect(new Tuple2<>(key, value.eventTimeStampMilliSecondsSinceEpoch));
            }
        }
    }

    @Override
    public boolean isHot(Tuple2<Long, Long> key) {
        long FIRST_AUCTION_ID = 1000l;
        long FIRST_PERSON_ID = 1000l;

        long auctionId = key.f1;
        long bidderId = key.f0;

        if (auctionId < FIRST_AUCTION_ID || bidderId < FIRST_PERSON_ID) return false;
        if ((auctionId - FIRST_AUCTION_ID) % hotIdsRatio == 0 && (bidderId - FIRST_PERSON_ID) % hotIdsPersonRatio == 0){ // hot key
            /*(if (key > lastHotKey) {
                lastHotAuction = key;
                return false;
            }*/
            return true;
        }
        return false;
    }
}
