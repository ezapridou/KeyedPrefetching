package net.michaelkoepf.spegauge.flink.queries.ysb;

import net.michaelkoepf.spegauge.api.common.model.sqbench.EntityRecordFull;
import net.michaelkoepf.spegauge.api.common.model.sqbench.NexmarkEvent;
import net.michaelkoepf.spegauge.api.common.model.sqbench.NexmarkJSONEvent;
import net.michaelkoepf.spegauge.api.common.model.sqbench.YSB_EventTs;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.prefetching.HintExtractorNexmarkJSON;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.prefetching.SaturatingCMS;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class HintExtractorYSB extends ProcessFunction<YSB_EventTs, Tuple2<Long, Long>> {
    SaturatingCMS cms = new SaturatingCMS(4, 10000, 8, 10, 1000);

    public HintExtractorYSB() {}

    @Override
    public void processElement(YSB_EventTs value, Context ctx, Collector<Tuple2<Long, Long>> out) throws Exception {
        long key = value.event.ad_id_long;
        // non-hot elements filter
        if (!cms.update(key)) {
            out.collect(new Tuple2<>(key, value.eventTimeStampMilliSecondsSinceEpoch));
        }
    }

}