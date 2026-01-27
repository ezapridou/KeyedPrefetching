package net.michaelkoepf.spegauge.flink.queries.ysb;

import net.michaelkoepf.spegauge.api.common.model.sqbench.EntityRecordFull;
import net.michaelkoepf.spegauge.api.common.model.sqbench.YSBJSONEvent;
import net.michaelkoepf.spegauge.api.common.model.sqbench.YSB_EventTs;
import net.michaelkoepf.spegauge.flink.queries.common.QueryGroup;
import net.michaelkoepf.spegauge.flink.queries.sqbench.QueryGroupWithConfig;
import net.michaelkoepf.spegauge.flink.queries.sqbench.config.DataAnalyticsQuery;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.QueryUtils;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.JSONToPOJO;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.LinkedList;
import java.util.List;

/**
 * Source --> Filter to keep only bids --> keyBy auction --> enrich each bid with value from static table (based on its auction)
 */
public class YSB_Prefetching extends QueryGroupWithConfig {

    @Override
    public String getName() {
        return "YSB";
    }

    @Override
    protected List<DataStream> register(StreamExecutionEnvironment env) throws Exception {

        DataStream<YSBJSONEvent> source =
                QueryUtils.newSourceStreamYSBJSON(
                                env, getHostname(), getPort(), getSourceParallelism())
                        .rebalance(); // force rebalance to ensure fair comparison even when downstream parallelism is sames as source parallelism

        SingleOutputStreamOperator<YSB_EventTs> mappedStream = source
                .process(new JSONToPojoYSB())
                .setParallelism(getParallelism())
                .name("JSONToPOJO");

        SingleOutputStreamOperator<Tuple2<Long, Long>> hintedStream = mappedStream
                .process(new HintExtractorYSB())
                .setParallelism(getParallelism())
                .name("HintExtractor")
                .returns(Types.TUPLE(Types.LONG, Types.LONG))
                .setBufferTimeout(0);

        DataStream<Tuple3<Long, String, Long>> projection = mappedStream
                .map(x -> new Tuple3<>(x.event.ad_id_long, x.event.adID, x.processingTimeMilliSecondsStart))
                .setParallelism(getParallelism())
                .name("Project")
                .returns(Types.TUPLE(Types.LONG, Types.STRING, Types.LONG));

        DataStream<Tuple3<Long, String, String>> result;

        result = hintedStream.keyBy(h -> h.f0)
                .connect(projection.keyBy(e -> e.f0))
                .process(new JoinRedisTablePrefetching())
                .setParallelism(getParallelism())
                .name("JoinRedis")
                .returns(Types.TUPLE(Types.LONG, Types.STRING, Types.STRING));

        List<DataStream> resultList = new LinkedList<>();
        resultList.add(result);

        return resultList;
    }
}
