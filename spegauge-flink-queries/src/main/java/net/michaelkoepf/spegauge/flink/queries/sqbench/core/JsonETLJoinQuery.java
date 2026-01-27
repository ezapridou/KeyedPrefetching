package net.michaelkoepf.spegauge.flink.queries.sqbench.core;

import net.michaelkoepf.spegauge.api.common.model.sqbench.EntityRecordFull;
import net.michaelkoepf.spegauge.flink.queries.common.QueryGroup;
import net.michaelkoepf.spegauge.flink.queries.sqbench.config.*;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.*;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.join.SingleInputJoinCache;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.LinkedList;
import java.util.List;

/**
 * Person and Auction Join
 */
public class JsonETLJoinQuery extends QueryGroup<Object> {

  public DataAnalyticsQuery queryConfig;

  public void setQueryConfig(DataAnalyticsQuery thisQuery) {
    this.queryConfig = thisQuery;
  }

  @Override
  public String getName() {
    return "JSON_ETL_JOIN_QUERY";
  }

  @Override
  protected List<DataStream> register(StreamExecutionEnvironment env) throws Exception {
    tempError();

    DataStream<EntityRecordFull> source =
            QueryUtils.newSourceStream(
                            env, getHostname(), getPort(), getSourceParallelism())
                    .rebalance(); // force rebalance to ensure fair comparison even when downstream parallelism is sames as source parallelism

    SingleOutputStreamOperator<EntityRecordFull> mappedStream = source
            .process(new JSONToPOJO())
            .setParallelism(getParallelism())
            .name("JSONToPOJO")
            .disableChaining();

    /*DataStream<EntityRecordFull> entityAJSON = QueryUtils.entityJSONA(getGroupId(), source, getParallelism());

    DataStream<EntityRecordFull> mappedStreamA = entityAJSON
            .process(new JSONToPOJO()).setParallelism(getParallelism()).name("AJSONToPOJOGID" + getGroupId())
            .disableChaining();

    DataStream<EntityRecordFull> entityBJSON = QueryUtils.entityJSONB(getGroupId(), source, getParallelism());

    DataStream<EntityRecordFull> mappedStreamB = entityBJSON
            .process(new JSONToPOJO()).setParallelism(getParallelism()).name("BJSONToPOJOGID" + getGroupId())
            .disableChaining();*/

    DataStream<Tuple2<EntityRecordFull, EntityRecordFull>> joinResult;

    joinResult = mappedStream
            .keyBy(e -> e.type == EntityRecordFull.Type.A ? e.PK : e.FK)
            .process(new SingleInputJoinCache(
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
