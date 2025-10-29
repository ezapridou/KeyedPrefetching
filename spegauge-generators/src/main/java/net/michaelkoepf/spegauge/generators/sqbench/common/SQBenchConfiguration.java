package net.michaelkoepf.spegauge.generators.sqbench.common;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Configuration file (i.e., a Java representation of a JSON file)
 */
public class SQBenchConfiguration implements Serializable {

    /**
     * Number of events to generate in total (across all generators).
     * If the number of generators does not divide this number evenly, the last generator will generate the remaining events.
     */
    @JsonProperty
    public long numEventsTotal;

    /**
     * List of scenarios to generate. At least one scenario must be specified.
     */
    @JsonProperty
    public List<Scenario> scenarios = new ArrayList<>();

    /**
     * Seed to initialize the random number generators.
     * The seed is not used directly, but used to generate seeds for the individual generators.
     */
    @JsonProperty
    public long seed;

    /**
     * Number of queries. At least one query must be specified.
     */
    @JsonProperty
    public int numOfQueries;

    /**
     * Definition of a scenario.
     */
    public static class Scenario {

        /** Name of the scenario (only for debugging/logging purposes) */
        @JsonProperty
        public String scenarioName;

        /** Switch to the next scenario if there is any after this many events have been generated (overall, *NOT* per generator).
         * This property will be ignored if the last scenario is reached.
         **/
        @JsonProperty
        public long nextScenarioAfter;

        /** Number of events to generate per second (overall, *NOT* per generator). Only effective if 'isRateLimited' is set true. */
        @JsonProperty
        public long numEventsPerSecond;

        /** Whether to generate/send events at the specified event raate (numEventsPerSecond) or as fast as possible */
        @JsonProperty
        public boolean isRateLimited;

        /** Proportion of entity A in the scenario (>= 0). */
        @JsonProperty
        public long proportionEntityA;

        /** Proportion of entity B in the scenario (>= 0). */
        @JsonProperty
        public long proportionEntityB;

        /** Proportion of entity C in the scenario (>= 0). */
        @JsonProperty
        public long proportionEntityC;

        /** Scaling factor for the proportions. */
        @JsonProperty
        public int proportionScalingFactor = 1;

        // TODO: you may add further entities here

        /** Event generation details for entity A. */
        @JsonProperty
        public SQBenchUtils.Entity entityA;

        /** Event generation details for entity B. */
        @JsonProperty
        public SQBenchUtils.Entity entityB;

        /** Event generation details for entity C. */
        @JsonProperty
        public SQBenchUtils.Entity entityC;

    }
}
