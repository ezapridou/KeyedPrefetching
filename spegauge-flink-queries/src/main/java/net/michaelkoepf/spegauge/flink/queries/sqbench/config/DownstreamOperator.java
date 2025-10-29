package net.michaelkoepf.spegauge.flink.queries.sqbench.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo( use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "metaTypeInfo")
@JsonSubTypes({
        @JsonSubTypes.Type(value = RollingReduce.class, name = "RollingReduce"),
        @JsonSubTypes.Type(value = WindowAggregation.class, name = "WindowAggregation"),
        @JsonSubTypes.Type(value = DataEnrichtmentFunction.class, name = "DataEnrichmentFunction"),
        @JsonSubTypes.Type(value = WindowedSumAndPOJOToX.class, name = "RollingSumAndPOJOToX")

})
public interface DownstreamOperator {

  @JsonIgnore
  boolean isStateful();

  default Object groupByField() {
    throw new UnsupportedOperationException("Not implemented");
  }

  default Object filterField() {
    throw new UnsupportedOperationException("Not implemented");
  }
}
