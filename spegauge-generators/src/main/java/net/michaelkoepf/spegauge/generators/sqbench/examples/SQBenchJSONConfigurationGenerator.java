package net.michaelkoepf.spegauge.generators.sqbench.examples;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import net.michaelkoepf.spegauge.api.common.model.sqbench.EntityRecordFull;
import net.michaelkoepf.spegauge.generators.sqbench.common.SQBenchConfiguration;
import net.michaelkoepf.spegauge.generators.sqbench.common.SQBenchUtils;

import java.util.List;

/**
 * This class shows an example how to easily build an initial JSON configuration file for SQBench programmatically.
 * Adapt it to your needs and run it in your IDE to generate the JSON configuration file.
 */
public class SQBenchJSONConfigurationGenerator {
    public static void main(String[] args) throws JsonProcessingException {
        SQBenchConfiguration configuration = new SQBenchConfiguration();
        configuration.numEventsTotal = 1000;
        configuration.seed = 42;

        SQBenchConfiguration.Scenario scenario = new SQBenchConfiguration.Scenario();
        scenario.scenarioName = "UnchangedWorkload";
        scenario.nextScenarioAfter = 1000;
        scenario.numEventsPerSecond = 1000000;
        scenario.proportionEntityA = 50;
        scenario.proportionEntityB = 50;
        scenario.proportionEntityC = 0;

        scenario.entityA = SQBenchUtils.Entity.builder()
                .entityType(EntityRecordFull.Type.A)
                .PK(SQBenchUtils.DiscreteAttribute.builder()
                        .distribution(new SQBenchUtils.Distribution(SQBenchUtils.Distribution.Type.UNIFORM_DISCRETE, List.of((double) 0, (double) 499)))
                        .numDistinctValues(500)
                        .build())
                .FK(null)
                .longAttribute1(SQBenchUtils.DiscreteAttribute.builder()
                        .distribution(new SQBenchUtils.Distribution(SQBenchUtils.Distribution.Type.UNIFORM_DISCRETE, List.of((double) 0, (double) 199)))
                        .numDistinctValues(200)
                        .build())
                .longAttribute2(SQBenchUtils.DiscreteAttribute.builder()
                        .distribution(new SQBenchUtils.Distribution(SQBenchUtils.Distribution.Type.UNIFORM_DISCRETE, List.of((double) 0, (double) 99)))
                        .numDistinctValues(100)
                        .build())
                .plainStringAttributeLength(SQBenchUtils.VariableLengthAttribute.builder()
                        .length(10)
                        .build())
                .jsonStringAttribute(new SQBenchUtils.ComplexPayloadAttribute(true))
                .xmlStringAttribute(new SQBenchUtils.ComplexPayloadAttribute()).build();

        scenario.entityB = SQBenchUtils.Entity.builder()
                .entityType(EntityRecordFull.Type.B)
                .PK(SQBenchUtils.DiscreteAttribute.builder()
                        .distribution(new SQBenchUtils.Distribution(SQBenchUtils.Distribution.Type.UNIFORM_DISCRETE, List.of((double) 0, (double) 99)))
                        .numDistinctValues(100)
                        .build())
                .FK(SQBenchUtils.ForeignKeyAttribute.builder()
                        .references(null)
                        .build())
                .longAttribute1(SQBenchUtils.DiscreteAttribute.builder()
                        .distribution(new SQBenchUtils.Distribution(SQBenchUtils.Distribution.Type.UNIFORM_DISCRETE, List.of((double) 0, (double) 9)))
                        .numDistinctValues(10)
                        .build())
                .longAttribute2(SQBenchUtils.DiscreteAttribute.builder()
                        .distribution(new SQBenchUtils.Distribution(SQBenchUtils.Distribution.Type.UNIFORM_DISCRETE, List.of((double) 0, (double) 19)))
                        .numDistinctValues(20)
                        .build())
                .plainStringAttributeLength(SQBenchUtils.VariableLengthAttribute.builder()
                        .length(400)
                        .build())
                .jsonStringAttribute(new SQBenchUtils.ComplexPayloadAttribute(true))
                .xmlStringAttribute(new SQBenchUtils.ComplexPayloadAttribute()).build();

        configuration.scenarios.add(scenario);

        ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
        String json = ow.writeValueAsString(configuration);
        System.out.println(json);
    }
}
