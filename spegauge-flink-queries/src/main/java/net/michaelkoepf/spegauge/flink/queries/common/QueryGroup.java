package net.michaelkoepf.spegauge.flink.queries.common;

import lombok.Getter;
import lombok.Setter;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

public abstract class QueryGroup<T> {

  // see
  // https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/operators/overview/#set-slot-sharing-group
  private static final String DEFAULT_SLOT_SHARING_GROUP = "default";

  private StreamExecutionEnvironment env;

  private boolean initialized = false;

  @Getter @Setter private String slotSharingGroup = DEFAULT_SLOT_SHARING_GROUP;

  @Getter private String hostname;

  @Getter private int port;

  @Getter private int sourceParallelism;

  @Getter private int parallelism;

  @Getter private int downstreamParallelism;

  @Getter private int groupId;

  // the number of concurrent queries
  @Getter private int numOfQueries;

  @Getter private long windowSizeMS;
  @Getter private long windowSlideMS;

  public QueryGroup() {}

  public void initialize(StreamExecutionEnvironment env, String hostname, int port, int groupId, int numOfQueries,
                         int parallelism, int downstreamParallelism) {
    this.env = env;
    this.hostname = hostname;
    this.port = port;
    this.groupId = groupId;
    this.numOfQueries = numOfQueries;

    ExecutionConfig.GlobalJobParameters jobParameters = this.env.getConfig().getGlobalJobParameters();
    sourceParallelism = Integer.parseInt(jobParameters.toMap().get("source.parallelism"));
    this.parallelism = parallelism;
    this.downstreamParallelism = downstreamParallelism;

    initialized = true;
  }

  public void initialize(StreamExecutionEnvironment env, String hostname, int port, int groupId, int numOfQueries) {
    initialize(env, hostname, port, groupId, numOfQueries, env.getParallelism(), env.getParallelism());
  }

  public abstract String getName();

  public List<DataStream> register() throws Exception {
    if (!initialized) {
      throw new IllegalStateException("Query was not initialized. Did you used the QueryFactory");
    }
    return register(env);
  }

  protected abstract List<DataStream> register(StreamExecutionEnvironment env) throws Exception;
}
