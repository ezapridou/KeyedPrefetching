package net.michaelkoepf.spegauge.flink.queries.sqbench.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@SuperBuilder(setterPrefix = "with")
@AllArgsConstructor
@NoArgsConstructor
public class FilterSpec {
    @JsonProperty
    public long startForEntityA;

    @JsonProperty
    public long endForEntityA;

    @JsonProperty
    public long startForEntityB;
    @JsonProperty
    public long endForEntityB;

    @JsonProperty
    public long startForEntityC;
    @JsonProperty
    public long endForEntityC;
}
