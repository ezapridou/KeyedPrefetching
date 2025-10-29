package net.michaelkoepf.spegauge.generators.sqbench;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.michaelkoepf.spegauge.api.driver.EventGenerator;
import net.michaelkoepf.spegauge.api.driver.EventGeneratorFactory;
import net.michaelkoepf.spegauge.generators.Config;
import net.michaelkoepf.spegauge.generators.sqbench.common.SQBenchConfiguration;
import org.apache.commons.rng.JumpableUniformRandomProvider;
import org.apache.commons.rng.UniformRandomProvider;
import org.apache.commons.rng.simple.RandomSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SQBenchEventGeneratorFactory extends EventGeneratorFactory {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(SQBenchEventGeneratorFactory.class);
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    private static final net.michaelkoepf.spegauge.api.driver.Config CONFIG = new Config();

    public SQBenchEventGeneratorFactory() {
        super();
        // do not remove this constructor
    }

    @Override
    public void setDataGenerationConfiguration(Map<String, Object> dataGenerationConfiguration) {
        if (!isInitialized()) {
            throw new IllegalStateException("Factory not initialized");
        }

        SQBenchConfiguration sqBenchConfiguration;
        sqBenchConfiguration = JSON_MAPPER.convertValue(dataGenerationConfiguration, SQBenchConfiguration.class);
        LOGGER.info(sqBenchConfiguration.scenarios.toString());

        // generate seeds from SPLIT MIX to avoid correlation
        // see https://prng.di.unimi.it/ (accessed 2024-02-25; section 64-bit generators at the end) or
        // https://dl.acm.org/doi/abs/10.1145/3460772 for details
        UniformRandomProvider seedRng = RandomSource.SPLIT_MIX_64.create(sqBenchConfiguration.seed);

        // generate non-overlapping sequences for parallel computations (see https://dl.acm.org/doi/abs/10.1145/3460772 for details)
        JumpableUniformRandomProvider jumpable = (JumpableUniformRandomProvider) CONFIG.getDefaultRandomSource().create(seedRng.nextLong());

        UniformRandomProvider[] rngs = new UniformRandomProvider[getNumberOfGenerators()];
        for (int i = 0; i < rngs.length; i++) {
               rngs[i] = jumpable.jump();
        }

        long basetime = 0L; // event time starts at the beginning of the unix epoch

        for (int i = 0; i < getNumberOfGenerators(); i++) {
            EventGenerator eventGenerator = new SQBenchEventGenerator(basetime, sqBenchConfiguration, i, getNumberOfGenerators(), rngs[i]);
            getEventGenerators().add(eventGenerator);
        }
    }

}
