package net.michaelkoepf.spegauge.flink.queries.sqbench.core;

import net.michaelkoepf.spegauge.api.common.model.sqbench.EntityRecordFull;
import net.michaelkoepf.spegauge.flink.queries.common.QueryGroup;
import net.michaelkoepf.spegauge.flink.queries.sqbench.config.DataAnalyticsQuery;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.prefetching.HintExtractor;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.JSONToPOJO;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.join.SingleInputJoinPrefetching;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

import java.util.LinkedList;
import java.util.List;

public class JsonETLJoinQueryPrefetching extends QueryGroup<Object> {

  public DataAnalyticsQuery queryConfig;

  public void setQueryConfig(DataAnalyticsQuery thisQuery) {
    this.queryConfig = thisQuery;
  }

  @Override
  public String getName() {
    return "JSON_ETL_JOIN_QUERY_PREFETCHING";
  }

  @Override
  protected List<DataStream> register(StreamExecutionEnvironment env) throws Exception {
    tempError();

    final OutputTag<Tuple3<Long, Long, Boolean>> metadataOutputTag = new OutputTag<Tuple3<Long, Long, Boolean>>("metadata") {};

    DataStream<EntityRecordFull> source =
            QueryUtils.newSourceStream(
                            env, getHostname(), getPort(), getSourceParallelism());
                    //.rebalance(); // force rebalance to ensure fair comparison even when downstream parallelism is sames as source parallelism

    SingleOutputStreamOperator<EntityRecordFull> hintedStream = source
            .process(new HintExtractor(metadataOutputTag))
            .setParallelism(16) // same parallelism as the source for chaining
            .name("HintExtractor")
            //.disableChaining()
            .setBufferTimeout(0);

    SingleOutputStreamOperator<EntityRecordFull> mappedStream = hintedStream
            .process(new JSONToPOJO())
            .setParallelism(getParallelism())
            .name("JSONToPOJO")
            .disableChaining();

    DataStream<Tuple3<Long, Long, Boolean>> metadataStream = hintedStream.getSideOutput(metadataOutputTag);

    DataStream<Tuple2<EntityRecordFull, EntityRecordFull>> joinResult;

    joinResult = metadataStream.keyBy(e -> e.f0)
            .connect(mappedStream
                    .keyBy(e -> e.type == EntityRecordFull.Type.A ? e.PK : e.FK))
            .process(new SingleInputJoinPrefetching(
                    queryConfig.joinOperators.windowSizeMs, queryConfig.joinOperators.windowSlideMs, false))
            .setParallelism(getParallelism())
            .name("Join")
            .disableChaining();

    List<DataStream> result = new LinkedList<>();
    result.add(joinResult);

    return result;
  }

  public void tempError() {
    throw(new RuntimeException("To use this query you must change the hint extractor to extract the attribute from the json " +
            "as the json does not contain PKs and FKs anymore. You also must change EntityRecordGenerator as now it is used for Nexmark"));
  }
}
