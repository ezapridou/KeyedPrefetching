package net.michaelkoepf.spegauge.flink.queries.sqbench.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@Builder(setterPrefix = "with")
@AllArgsConstructor
@NoArgsConstructor
public class QueryConfig {

  @JsonProperty
  @Builder.Default
  public List<DataAnalyticsQuery> dataAnalyticsQueries = new ArrayList<>();

  @JsonProperty
  @Builder.Default
  public List<StreamingETLQuery> streamingETLQueries = new ArrayList<>();

}
