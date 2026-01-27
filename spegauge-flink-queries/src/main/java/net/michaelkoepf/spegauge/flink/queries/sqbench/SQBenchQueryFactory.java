package net.michaelkoepf.spegauge.flink.queries.sqbench;

import net.michaelkoepf.spegauge.flink.queries.common.QueryGroup;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.*;
import net.michaelkoepf.spegauge.flink.queries.ysb.YSB;
import net.michaelkoepf.spegauge.flink.queries.ysb.YSB_Prefetching;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

public final class SQBenchQueryFactory {

  private static final Map<String, Supplier<QueryGroup>> FACTORY =
      Collections.unmodifiableMap(
          // LinkedHashMap to preserve order when getting keys for IMPLEMENTED_QUERIES (because it
          // used when printing the help message; otherwise, this will be confusing for the user)
          new LinkedHashMap<String, Supplier<QueryGroup>>() {
            {
              put("JSON_ETL_JOIN_QUERY", JsonETLJoinQuery::new);
              put("JSON_ETL_JOIN_QUERY_PREFETCHING", JsonETLJoinQueryPrefetching::new);

              put("QUERY20", Q20::new);
              put("QUERY20_PREFETCHING", Q20Prefetching::new);

              put("QUERY13", Q13::new);
              put("QUERY13_PREFETCHING", Q13Prefetching::new);

              put("QUERY18", Q18::new);
              put("QUERY18_PREFETCHING", Q18Prefetching::new);

              put("QUERY19", Q19::new);
              put("QUERY19_PREFETCHING", Q19Prefetching::new);

              put("YSB", YSB::new);
              put("YSB_PREFETCHING", YSB_Prefetching::new);
            }
          });

  public static final Set<String> IMPLEMENTED_QUERIES =
      Collections.unmodifiableSet(FACTORY.keySet());

  private SQBenchQueryFactory() {}

  public static QueryGroup<?> getQuery(
      String queryName, StreamExecutionEnvironment env, String hostname, int port, int groupId, int numOfQueries,
      int parallelism, int downstreamParallelism) {
    Supplier<QueryGroup> supplier = FACTORY.get(queryName);

    if (supplier == null) {
      throw new IllegalStateException("Unknown query " + queryName);
    } else {
      QueryGroup<?> query = supplier.get();
      query.initialize(env, hostname, port, groupId, numOfQueries, parallelism, downstreamParallelism);
      return query;
    }
  }

  public static QueryGroup<?> getQuery(
          String queryName, StreamExecutionEnvironment env, String hostname, int port, int groupId, int numOfQueries) {
    return getQuery(queryName, env, hostname, port, groupId, numOfQueries, env.getParallelism(), env.getParallelism());
  }
}
