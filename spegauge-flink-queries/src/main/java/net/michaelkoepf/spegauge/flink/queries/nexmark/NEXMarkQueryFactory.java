package net.michaelkoepf.spegauge.flink.queries.nexmark;

import net.michaelkoepf.spegauge.flink.queries.common.QueryGroup;
import net.michaelkoepf.spegauge.flink.queries.nexmark.core.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

public final class NEXMarkQueryFactory {

  private static final Map<String, Supplier<QueryGroup>> FACTORY =
      Collections.unmodifiableMap(
          // LinkedHashMap to preserve order when getting keys for IMPLEMENTED_QUERIES (because it
          // used when printing the help message; otherwise, this will be confusing for the user)
          new LinkedHashMap<String, Supplier<QueryGroup>>() {
            {
              put("LOAD_TEST", LoadTest::new);
              put("MAP_LONG_MICRO_BENCHMARK", MapLongMicroBenchmark::new);
              put("MAP_STRING_MICRO_BENCHMARK", MapStringMicroBenchmark::new);
              put("FILTER_MICRO_BENCHMARK", FilterMicroBenchmark::new);
              put("WINDOW_JOIN_WITH_AGGREGATION", WindowJoinWithAggregation::new);
              //              put("Query2", Query2::new);
              //              put("SELECTION", Query2::new);
              //              put("Query3", Query3::new);
              //              put("LOCAL_ITEM_SUGGESTION", Query3::new);
              //              put("Query3CoProcess", Query3_CoProcessFunction::new);
              //              put("LOCAL_ITEM_SUGGESTION_CoProcess", Query3_CoProcessFunction::new);
              put("Query4", Query4::new);
              put("AVERAGE_PRICE_FOR_CATEGORY", Query4::new);
              //              put("Query5", Query5::new);
              //              put("HOT_ITEMS", Query5::new);
              put("Query6", Query6::new);
              put("AVERAGE_SELLING_PRICE_BY_SELLER", Query6::new);
              //              put("Query7", Query7::new);
              //              put("HIGHEST_BID", Query7::new);
              //              put("Query8", Query8::new);
              //              put("MONITOR_NEW_USERS", Query8::new);
              //              put("Query9", Query9::new);
              //              put("WINNING_BIDS", Query9::new);
              //              put("Query4Query6SameJob", Query4Query6SameJob::new);
            }
          });

  public static final Set<String> IMPLEMENTED_QUERIES =
      Collections.unmodifiableSet(FACTORY.keySet());

  private NEXMarkQueryFactory() {}

  public static QueryGroup<?> getQuery(
      String queryName, StreamExecutionEnvironment env, String hostname, int port, int groupId, int numOfQueries) {
    Supplier<QueryGroup> supplier = FACTORY.get(queryName);

    if (supplier == null) {
      throw new IllegalStateException("Unknown query " + queryName);
    } else {
      QueryGroup<?> query = supplier.get();
      query.initialize(env, hostname, port, groupId, numOfQueries);
      return query;
    }
  }
}
