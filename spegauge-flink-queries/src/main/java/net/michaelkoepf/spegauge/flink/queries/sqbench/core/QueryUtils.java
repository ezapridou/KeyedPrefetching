package net.michaelkoepf.spegauge.flink.queries.sqbench.core;

import net.michaelkoepf.spegauge.api.QueryConfig;
import net.michaelkoepf.spegauge.api.common.model.sqbench.EntityRecordFull;
import net.michaelkoepf.spegauge.api.common.model.sqbench.EntityRecordLimitedAttrs;
import net.michaelkoepf.spegauge.api.common.model.sqbench.NexmarkJSONEvent;
import net.michaelkoepf.spegauge.api.common.model.sqbench.YSBJSONEvent;
import net.michaelkoepf.spegauge.flink.queries.common.QueryGroup;
import net.michaelkoepf.spegauge.flink.queries.sqbench.QueryGroupWithConfig;
import net.michaelkoepf.spegauge.flink.queries.sqbench.SQBenchQueryFactory;
import net.michaelkoepf.spegauge.flink.queries.sqbench.config.DataAnalyticsQuery;
import net.michaelkoepf.spegauge.flink.queries.sqbench.config.Operator;
import net.michaelkoepf.spegauge.flink.queries.ysb.YSB;
import net.michaelkoepf.spegauge.flink.queries.ysb.YSB_Prefetching;
import net.michaelkoepf.spegauge.flink.sdk.source.TCPSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public final class QueryUtils {

  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(QueryUtils.class);
  private static final AtomicInteger sourceNumber = new AtomicInteger();

  private QueryUtils() {}

  /**
   * Returns a new source that is in its own slot sharing group.
   *
   * @param env
   * @param hostname
   * @param port
   * @param parallelism
   * @return
   */
  public static DataStream<EntityRecordFull> newSourceStream(
          StreamExecutionEnvironment env,
          String hostname,
          int port,
          int parallelism) {
    TCPSource<EntityRecordFull> source = new TCPSource<EntityRecordFull>(hostname, port, false, true);
    WatermarkStrategy<EntityRecordFull> wmStrategy = WatermarkStrategy
            .<EntityRecordFull>forMonotonousTimestamps()
            .withTimestampAssigner((event, timestamp) -> event.eventTimeStampMilliSecondsSinceEpoch);
    return env.fromSource(source, wmStrategy, "TCP Socket Source")
            .setParallelism(parallelism)
            .setMaxParallelism(parallelism)
//      .slotSharingGroup("sourceGroup" + sourceNumber.getAndIncrement())
            .name("SQBenchParallelTCPSource").returns(EntityRecordFull.class);
  }

  public static DataStream<YSBJSONEvent> newSourceStreamYSBJSON(
          StreamExecutionEnvironment env,
          String hostname,
          int port,
          int parallelism) {
    TCPSource<YSBJSONEvent> source = new TCPSource<YSBJSONEvent>(hostname, port, false, true);
    WatermarkStrategy<YSBJSONEvent> wmStrategy = WatermarkStrategy
            .<YSBJSONEvent>forMonotonousTimestamps()
            .withTimestampAssigner((event, timestamp) -> event.eventTimeStampMilliSecondsSinceEpoch);
    return env.fromSource(source, wmStrategy, "TCP Socket Source")
            .setParallelism(parallelism)
            .setMaxParallelism(parallelism)
//      .slotSharingGroup("sourceGroup" + sourceNumber.getAndIncrement())
            .name("SQBenchParallelTCPSource").returns(YSBJSONEvent.class);
  }

  public static DataStream<NexmarkJSONEvent> newSourceStreamNexmarkJSON(
          StreamExecutionEnvironment env,
          String hostname,
          int port,
          int parallelism) {
    TCPSource<NexmarkJSONEvent> source = new TCPSource<NexmarkJSONEvent>(hostname, port, false, true);
    WatermarkStrategy<NexmarkJSONEvent> wmStrategy = WatermarkStrategy
            .<NexmarkJSONEvent>forMonotonousTimestamps()
            .withTimestampAssigner((event, timestamp) -> event.eventTimeStampMilliSecondsSinceEpoch);
    return env.fromSource(source, wmStrategy, "TCP Socket Source")
            .setParallelism(parallelism)
            .setMaxParallelism(parallelism)
//      .slotSharingGroup("sourceGroup" + sourceNumber.getAndIncrement())
            .name("NexmarkJSONParallelTCPSource").returns(NexmarkJSONEvent.class);
  }

  public static DataStream<NexmarkJSONEvent> newSourceStreamNexmarkJSON(
          StreamExecutionEnvironment env,
          String hostname,
          int port,
          int parallelism,
          boolean markersOn) {
    TCPSource<NexmarkJSONEvent> source = new TCPSource<NexmarkJSONEvent>(hostname, port, markersOn, true);
    WatermarkStrategy<NexmarkJSONEvent> wmStrategy = WatermarkStrategy
            .<NexmarkJSONEvent>forMonotonousTimestamps()
            .withTimestampAssigner((event, timestamp) -> event.eventTimeStampMilliSecondsSinceEpoch);
    return env.fromSource(source, wmStrategy, "TCP Socket Source")
            .setParallelism(parallelism)
            .setMaxParallelism(parallelism)
//      .slotSharingGroup("sourceGroup" + sourceNumber.getAndIncrement())
            .name("NexmarkJSONParallelTCPSource").returns(NexmarkJSONEvent.class);
  }

  public static DataStream<EntityRecordLimitedAttrs> newSourceStreamJoinNoExtraAttrs(
          StreamExecutionEnvironment env,
          String hostname,
          int port,
          int parallelism) {
    TCPSource<EntityRecordLimitedAttrs> source = new TCPSource<EntityRecordLimitedAttrs>(hostname, port, true, false);
    WatermarkStrategy<EntityRecordLimitedAttrs> wmStrategy = WatermarkStrategy
            .<EntityRecordLimitedAttrs>forMonotonousTimestamps()
            .withTimestampAssigner((event, timestamp) -> event.eventTimeStampMilliSecondsSinceEpoch);
    return env.fromSource(source, wmStrategy, "TCP Socket Source")
            .setParallelism(parallelism)
            .setMaxParallelism(parallelism)
//        .slotSharingGroup("sourceGroup" + sourceNumber.getAndIncrement())
            .name("SQBenchParallelTCPSource");
    //.slotSharingGroup("slotGroup" + groupId%4);
  }

  public static DataStream<EntityRecordFull> entityA(DataStream<EntityRecordFull> source) {
    return source.filter(e -> e.type == EntityRecordFull.Type.A).name("EntityA");
  }

  public static DataStream<EntityRecordFull> entityB(DataStream<EntityRecordFull> source) {
    return source.filter(e -> e.type == EntityRecordFull.Type.B).name("EntityB");
  }

  public static DataStream<EntityRecordFull> entityA(DataStream<EntityRecordFull> source, int parallelism) {
    return source.filter(e -> e.type == EntityRecordFull.Type.A).setParallelism(parallelism).name("EntityA");
  }

  public static DataStream<EntityRecordFull> entityB(DataStream<EntityRecordFull> source, int parallelism) {
    return source.filter(e -> e.type == EntityRecordFull.Type.B).setParallelism(parallelism).name("EntityB");
  }

  public static DataStream<EntityRecordFull> entityA(int groupId, DataStream<EntityRecordFull> source, int parallelism) {
    return source.filter(e -> e.type == EntityRecordFull.Type.A).name("EntityAGID" + groupId).setParallelism(parallelism);
  }

  public static DataStream<EntityRecordFull> entityB(int groupId, DataStream<EntityRecordFull> source, int parallelism) {
    return source.filter(e -> e.type == EntityRecordFull.Type.B).name("EntityBGID" + groupId).setParallelism(parallelism);
  }

  public static DataStream<EntityRecordFull> entityJSONA(int groupId, DataStream<EntityRecordFull> source, int parallelism) {
    return source.filter(e -> e.type == EntityRecordFull.Type.JSONA).name("EntityJSONAGID" + groupId).setParallelism(parallelism);
  }

  public static DataStream<EntityRecordFull> entityJSONB(int groupId, DataStream<EntityRecordFull> source, int parallelism) {
    return source.filter(e -> e.type == EntityRecordFull.Type.JSONB).name("EntityJSONBGID" + groupId).setParallelism(parallelism);
  }

  public static DataStream<EntityRecordLimitedAttrs> entityAJoin(int groupId, DataStream<EntityRecordLimitedAttrs> source, int parallelism) {
    return source.filter(e -> e.isOfTypeA).name("EntityAGID" + groupId).setParallelism(parallelism);//.slotSharingGroup("slotGroup" + groupId%4);
  }

  public static DataStream<EntityRecordLimitedAttrs> entityBJoin(int groupId, DataStream<EntityRecordLimitedAttrs> source, int parallelism) {
    return source.filter(e -> !e.isOfTypeA).name("EntityBGID" + groupId).setParallelism(parallelism);//.slotSharingGroup("slotGroup" + groupId%4);
  }

  public static DataStream<EntityRecordFull> entityC(DataStream<EntityRecordFull> source) {
    return source.filter(e -> e.type == EntityRecordFull.Type.C).name("EntityC");
  }

  public static DataStream<EntityRecordFull> entityC(DataStream<EntityRecordFull> source, int parallelism) {
    return source.filter(e -> e.type == EntityRecordFull.Type.C).setParallelism(parallelism).name("EntityC");
  }

  public static DataStream<EntityRecordFull> entityC(int groupId, DataStream<EntityRecordFull> source, int parallelism) {
    return source.filter(e -> e.type == EntityRecordFull.Type.C).name("EntityCGID" + groupId).setParallelism(parallelism);
  }

  public static final class JSONConfig {
    public static List<QueryGroup<Object>> buildQueries(StreamExecutionEnvironment env, List<String> driverInformation, List<?> queries, int gidOffset, int numTotalQueries, Class<?> clazz) throws Exception {
      List<QueryGroup<Object>> result = new LinkedList<>();

      for (int i = 0; i < queries.size(); i++) {
        String hostPort = driverInformation.get(i);
        String hostname = hostPort.split(":")[0];
        int port = Integer.parseInt(hostPort.split(":")[1]);
        LOGGER.info(
                "Processing data analytics query group " + i + "(driver hostname " + hostname + ", driver port: " + port);

        // TODO: number of queries per query type (data analytics streaming etl) or overall? at the moment, it's the FORMER
        int downstreamParallelismFactor = 1;
        if (clazz == DataAnalyticsQuery.class) {
          if ((Operator) ((DataAnalyticsQuery)queries.get(i)).downstreamOperator != null) {
            downstreamParallelismFactor = ((Operator) ((DataAnalyticsQuery)queries.get(i)).downstreamOperator).parallelismFactor;
          }

          String queryName = QueryConfig.SELECTED_QUERY.name();

          var q = (QueryGroupWithConfig) SQBenchQueryFactory.getQuery(queryName, env, hostname, port, i + gidOffset, numTotalQueries, env.getParallelism() * ((DataAnalyticsQuery) queries.get(i)).joinOperators.parallelismFactor, env.getParallelism() * downstreamParallelismFactor);

          q.setQueryConfig((DataAnalyticsQuery) queries.get(i));
          result.add(q);
        } else {
          throw new IllegalStateException("Unknown query class: " + clazz);
        }
      }

      return result;
    }
  }
}
