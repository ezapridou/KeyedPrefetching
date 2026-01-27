package net.michaelkoepf.spegauge.flink.queries.sqbench.config;

import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@SuperBuilder(setterPrefix = "with")
@AllArgsConstructor
@NoArgsConstructor
public class DataAnalyticsQuery implements Shareable, ConfigurableQuery {

  @JsonProperty
  public WindowJoin joinOperators;

  @JsonProperty
  public WindowAggregation aggregationOperator;

  @JsonProperty
  public DownstreamOperator downstreamOperator;

  @JsonProperty
  public FilterSpec filter;

  /**
   * Instances of this class are shareable if they have the same join operators.
   * @param o The object to compare this instance to.
   * @return True if the objects are shareable, false otherwise.
   */
  @Override
  public boolean isShareable(Shareable o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DataAnalyticsQuery otherQuery = (DataAnalyticsQuery) o;
    return joinOperators.equals(otherQuery.joinOperators);
  }

}
