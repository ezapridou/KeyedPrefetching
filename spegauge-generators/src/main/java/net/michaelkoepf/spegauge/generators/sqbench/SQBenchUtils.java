package net.michaelkoepf.spegauge.generators.sqbench;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.ToString;
import net.michaelkoepf.spegauge.api.common.model.sqbench.EntityRecordFull;
import org.apache.commons.rng.UniformRandomProvider;
import org.apache.commons.statistics.distribution.*;

import java.io.Serializable;
import java.util.List;

public final class SQBenchUtils {

    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @ToString
    public static class Distribution implements Serializable {
        public enum Type {
            BETA("beta"),
            UNIFORM_CONTINUOUS("uniform_continuous"),
            UNIFORM_DISCRETE("uniform_discrete"),
            ZIPF("zipf");

            public final String value;

            Type(String value) {
                this.value = value;
            }
        }

        public Type type;
        public List<Double> parameters;

        @JsonIgnore
        public static ContinuousDistribution.Sampler getContinuousDistributionSampler(Type type, List<Double> parameters, UniformRandomProvider rng) {
            switch (type.value) {
                case "beta":
                    return BetaDistribution.of(parameters.get(0), parameters.get(1)).createSampler(rng);
                case "uniform_continuous":
                    return UniformContinuousDistribution.of(parameters.get(0), parameters.get(1)).createSampler(rng);
                default:
                    return null;
            }
        }

        @JsonIgnore
        public static DiscreteDistribution.Sampler getDiscreteDistributionSampler(Type type, List<Double> parameters, UniformRandomProvider rng) {
            switch (type.value) {
                case "uniform_discrete":
                    return UniformDiscreteDistribution.of(parameters.get(0).intValue(), parameters.get(1).intValue()).createSampler(rng);
                case "zipf":
                    return ZipfDistribution.of(parameters.get(0).intValue(), parameters.get(1)).createSampler(rng);
                default:
                    return null;
            }
        }
    }

    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @ToString
    public static class Entity implements Serializable {

        @JsonProperty
        public EntityRecordFull.Type entityType;

        @JsonProperty
        public DiscreteAttribute PK;

        @JsonProperty
        public ForeignKeyAttribute FK;

        @JsonProperty
        public DiscreteAttribute longAttribute1;

        @JsonProperty
        public DiscreteAttribute longAttribute2;

        @JsonProperty
        public VariableLengthAttribute plainStringAttributeLength;

        @JsonProperty
        public ComplexPayloadAttribute jsonStringAttribute;

        @JsonProperty
        public ComplexPayloadAttribute xmlStringAttribute;
    }

    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @ToString
    public static class DiscreteAttribute {
        @JsonProperty
        public Distribution distribution;

        @JsonProperty
        public int numDistinctValues;
    }

    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @ToString
    public static class ForeignKeyAttribute {
        @JsonProperty
        public EntityRecordFull.Type references;

        @JsonProperty
        public Distribution distribution;
    }

    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @ToString
    public static class VariableLengthAttribute {
        @JsonProperty
        public int length;
    }

    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @ToString
    public static class ComplexPayloadAttribute {
        // TODO: for now it can only be turned on an off and fields are hardcoded in the generator
        @JsonProperty
        public boolean enabled;
    }
}
