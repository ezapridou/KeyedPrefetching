package net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.prefetching;

import net.michaelkoepf.spegauge.api.common.model.sqbench.NexmarkEvent;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public abstract class HintExtractorNexmark extends ProcessFunction<NexmarkEvent, Tuple3<Long, Long, Boolean>>{
    private final boolean hintsForAuction;

    public long lastHotAuction = 0;

    SaturatingCMS cms = new SaturatingCMS();

    public abstract long getKey(NexmarkEvent event);
    public abstract boolean isHot(long key);

    public HintExtractorNexmark(boolean hintsForAuction) {
        this.hintsForAuction = hintsForAuction;
    }

    @Override
    public void processElement(NexmarkEvent value, Context ctx, Collector<Tuple3<Long, Long, Boolean>> out) throws Exception {
        if ((value.type == NexmarkEvent.Type.AUCTION) == hintsForAuction) {
            long key = getKey(value);
            // non-hot elements filter
            // key % hot_seller_ratio != first_person_id
            if (!cms.update(key)) {
                out.collect(new Tuple3<>(key, value.eventTimeStampMilliSecondsSinceEpoch, hintsForAuction));
            }
        }

    }
}
