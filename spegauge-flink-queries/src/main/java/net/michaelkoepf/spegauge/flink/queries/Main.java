package net.michaelkoepf.spegauge.flink.queries;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.michaelkoepf.spegauge.api.common.ObservableEvent;
import net.michaelkoepf.spegauge.flink.queries.common.QueryGroup;
import net.michaelkoepf.spegauge.flink.queries.nexmark.NEXMarkQueryFactory;
import net.michaelkoepf.spegauge.flink.queries.sqbench.SQBenchQueryFactory;
import net.michaelkoepf.spegauge.flink.queries.sqbench.config.DataAnalyticsQuery;
import net.michaelkoepf.spegauge.flink.queries.sqbench.config.FixedConfigGenerator;
import net.michaelkoepf.spegauge.flink.queries.sqbench.config.QueryConfig;
import net.michaelkoepf.spegauge.flink.queries.sqbench.config.StreamingETLQuery;
import net.michaelkoepf.spegauge.flink.queries.sqbench.core.QueryUtils;
import net.michaelkoepf.spegauge.flink.sdk.sink.EventTimeLatencyMeter;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.util.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@CommandLine.Command(
    name = "benchmark-flink",
    mixinStandardHelpOptions = true,
    description = "Benchmark")
public class Main implements Runnable {
  private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @CommandLine.Option(
      names = {"-m", "--module"},
      required = true,
      description = "Module")
  private String module = "benchmark";

  @CommandLine.Option(
      names = {"-s", "--source-parallelism"},
      description = "Source parallelism")
  private int sourceParallelism = 16;

  @CommandLine.Option(
          names = {"--max-parallelism"},
          description = "Max parallelism")
  private int maxParallelism = 8;

  @CommandLine.Option(
      names = {"-d", "--driver"},
      required = true,
      arity = "1..*",
      description =
          "<hostname>:<port> pairs (must match the number of queries given as positional arguments")
  private List<String> driver;

  @CommandLine.Option(
          names = {"--driver-api-port"},
          description = "Web API port of the driver")
  private int driverAPIPort = 8100;

  @CommandLine.Option(
      names = {"-c", "--config-path"},
      description = "Path to configuration file (default: flink.config in /resources)")
  private String flinkConfigPath = "/flink.config";

  @CommandLine.Parameters(
      index = "0",
      arity = "1..*",
      description = "Names of queries that should be executed OR path to the JSON configuration file")
  private List<String> queriesToBeExecuted;

  @CommandLine.Option(
          names = {"--csv-sink"},
          arity = "0",
          description = "Emit results to csv sink")
  private boolean csvSink;

  @CommandLine.Option(
          names = {"--measure-event-time-latency"},
          arity = "0",
          description = "Measures event time latency and sends results to driver")
  private boolean measureEventTimeLatency;

  @CommandLine.Option(
          names = {"--event-time-latency-params"},
          split = ",",
          description = "Tuple of <hostname>:<port>,<sample-interval> for event time latency measurement. Only used if --measure-event-time-latency is set.")
  private List<Object> eventTimeLatencyParams = new LinkedList<>();

  @CommandLine.Option(
          names = {"--csv-sink-base-path"},
          arity = "1",
          description = "Base path where CSV results should be stored (use absolute path!)")
  private String csvSinkBasePath = "/scratch/koepf/measurements/";

  @CommandLine.Option(
          names = {"--csv-sink-filename"},
          required = false,
          arity = "1",
          description = "Emit results to csv sink")
  private String csvSinkFileName;

  public static void main(String[] args) {
    new CommandLine(new Main()).execute(args);
  }

  @Override
  public void run() {
    QueryConfig queryConfig = null;
    // TODO: validate arguments in own method
    if (queriesToBeExecuted.size() == 1 && queriesToBeExecuted.get(0).endsWith(".json")) {
      LOGGER.info("Queries given by JSON configuration file " + queriesToBeExecuted.get(0));
      try {
        queryConfig = OBJECT_MAPPER.readValue(Main.class.getResourceAsStream("/" + queriesToBeExecuted.get(0)), QueryConfig.class);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }

      if (driver.size() != (queryConfig.dataAnalyticsQueries.size() + queryConfig.streamingETLQueries.size())) {
        throw new IllegalArgumentException(
                "Number of queries must match number of drivers. Given number of drivers: "
                        + driver.size()
                        + ". Given number of queries: "
                        + (queryConfig.dataAnalyticsQueries.size() + queryConfig.streamingETLQueries.size()));
      }
    } else if (queriesToBeExecuted.size() == 1 && queriesToBeExecuted.get(0).startsWith("hardcoded-logic")) {
      LOGGER.info("Queries given by hardcoded logic");
      String[] temp = queriesToBeExecuted.get(0).split("-");
      if (temp.length != 3) {
        queryConfig = FixedConfigGenerator.getConfig();
      }
      else {
        int selectivity = Integer.parseInt(temp[temp.length - 1]);
        queryConfig = FixedConfigGenerator.getConfig(selectivity);
      }
    } else {
      LOGGER.info("Queries given by name");
      if (queriesToBeExecuted.size() != driver.size()) {
        throw new IllegalArgumentException(
                "Number of queries must match number of drivers. Given number of drivers: "
                        + driver.size()
                        + ". Given number of queries: "
                        + queriesToBeExecuted.size());
      }
    }

    if (csvSink && measureEventTimeLatency) {
      throw new IllegalArgumentException("Cannot enable both csv sink and event time latency measurement");
    }

    if (measureEventTimeLatency && eventTimeLatencyParams.size() != 2) {
      throw new IllegalArgumentException("--event-time-latency-params must be a tuple of <hostname>:<port>,<sample-interval>. Got " + eventTimeLatencyParams.size());
    }

    LOGGER.info("Starting...");

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // overwrite configuration by command line args (if given)
    Map<String, String> updatedParams = null;
    try {
      updatedParams =
          new HashMap<>(
              ParameterTool.fromPropertiesFile(Main.class.getResourceAsStream(flinkConfigPath))
                  .toMap());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    updatedParams.put("source.parallelism", Integer.toString(sourceParallelism));
    updatedParams.put("driverPort", driverAPIPort + "");
    env.getConfig().setGlobalJobParameters(ParameterTool.fromMap(updatedParams));
    env.setMaxParallelism(maxParallelism);
//    env.getConfig().enableObjectReuse();

    switch (module) {
      case "nexmark":
        LOGGER.info("Chosen module: nexmark");
        throw new IllegalArgumentException("Nexmark is not supported anymore"); // TODO: check if implementation needs to be updated
//        try {
//          nexmark(env);
//        } catch (Exception e) {
//          throw new RuntimeException(e);
//        }
//        break;
      case "sqbench":
        LOGGER.info("Chosen module: sqbench");
        try {
          if (queryConfig == null) {
            sqbenchFromQueryNames(env);
          } else {
            sqbenchFromConfig(env, queryConfig);
          }
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        break;
      default:
        throw new IllegalArgumentException("Unknown module " + module);
    }
  }

  private void nexmark(StreamExecutionEnvironment env) throws Exception {
    List<QueryGroup<?>> queries = new LinkedList<>();
    for (int i = 0; i < queriesToBeExecuted.size(); i++) {
      String queryName = queriesToBeExecuted.get(i);
      String hostPort = driver.get(i);
      String hostname = hostPort.split(":")[0];
      int port = Integer.parseInt(hostPort.split(":")[1]);
      LOGGER.info(
          "Getting query " + queryName + "(driver hostname " + hostname + ", driver port: " + port);
      queries.add(NEXMarkQueryFactory.getQuery(queryName, env, hostname, port, i, queriesToBeExecuted.size()));
    }

    List<String> names = new LinkedList<>();
    for (QueryGroup<?> query : queries) {
      query.register();
      names.add(query.getName());
    }

    env.execute(String.join(" & ", names));
  }

  private void sqbenchFromQueryNames(StreamExecutionEnvironment env) throws Exception {
      List<QueryGroup<?>> queries = new LinkedList<>();

      for (int i = 0; i < queriesToBeExecuted.size(); i++) {
        String queryName = queriesToBeExecuted.get(i);
        String hostPort = driver.get(i);
        String hostname = hostPort.split(":")[0];
        int port = Integer.parseInt(hostPort.split(":")[1]);
        LOGGER.info(
                "Getting query " + queryName + "(driver hostname " + hostname + ", driver port: " + port);

        // TODO: for now we assume that the first query will be the active one when sharing all queries
        if (i == 0) {
          queries.add(SQBenchQueryFactory.getQuery(queryName, env, hostname, port, i, queriesToBeExecuted.size(), env.getParallelism() * 2, env.getParallelism()));
        } else {
          queries.add(SQBenchQueryFactory.getQuery(queryName, env, hostname, port, i, queriesToBeExecuted.size()));
        }
      }

      List<String> names = new LinkedList<>();
      for (QueryGroup<?> query : queries) {
        var registered = query.register();

        if (csvSink) {
          for (var r :
                  registered) {
            throw new UnsupportedOperationException("Not supported atm");
            //r.writeAsText(csvSinkBasePath + csvSinkFileName, FileSystem.WriteMode.NO_OVERWRITE).disableChaining();
          }
        }

        if (measureEventTimeLatency) {
          for (var r :
                  registered) {

            @SuppressWarnings("unchecked")
            // unchecked cast will throw exception if query does not return DataStream<EventTimeLatencyMeterObject<?>>
            DataStream<ObservableEvent> maybeDatastream = (DataStream<ObservableEvent>) r;

            if (eventTimeLatencyParams.get(0).equals("log")) {
              maybeDatastream.map(new EventTimeLatencyMeter(Long.parseLong((String) eventTimeLatencyParams.get(1))));
            } else {
              String[] host_port = ((String) eventTimeLatencyParams.get(0)).split(":");
              maybeDatastream.map(new EventTimeLatencyMeter(host_port[0], Integer.parseInt(host_port[1]), (long) eventTimeLatencyParams.get(1)));
            }
          }
        }

        names.add(query.getName());
      }

      env.execute(String.join(" & ", names));
  }

  private void sqbenchFromConfig(StreamExecutionEnvironment env, QueryConfig queryConfig) throws Exception {
    List<QueryGroup<Object>> queries = new LinkedList<>();

    int numTotalQueries = queryConfig.dataAnalyticsQueries.size() + queryConfig.streamingETLQueries.size();

    var dataAnalyticsQueries = QueryUtils.JSONConfig.buildQueries(env, driver.subList(0, queryConfig.dataAnalyticsQueries.size()),
            queryConfig.dataAnalyticsQueries, 0, numTotalQueries, DataAnalyticsQuery.class);
    queries.addAll(dataAnalyticsQueries);

    var streamingETLQueries = QueryUtils.JSONConfig.buildQueries(env, driver.subList(queryConfig.dataAnalyticsQueries.size(),
            driver.size()), queryConfig.streamingETLQueries, queryConfig.dataAnalyticsQueries.size(), numTotalQueries, StreamingETLQuery.class);
    queries.addAll(streamingETLQueries);

    List<String> names = new LinkedList<>();
    for (var q: queries) {
      var registered = q.register();

      if (csvSink) {
        throw new UnsupportedOperationException("CSV sink currently not supported for data analytics queries");
      }

      if (measureEventTimeLatency) {
        throw new UnsupportedOperationException("Event time latency measurement currently not supported for data analytics queries");
      }

      names.add(q.getName());
    }

    env.execute(String.join(" & ", names));

  }
}
