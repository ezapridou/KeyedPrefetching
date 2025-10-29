package net.michaelkoepf.spegauge.generators.nexmark;

import net.michaelkoepf.spegauge.api.driver.EventGeneratorFactory;
import net.michaelkoepf.spegauge.generators.nexmark.util.GeneratorConfig;
import net.michaelkoepf.spegauge.generators.nexmark.util.NexmarkConfiguration;
import net.michaelkoepf.spegauge.generators.nexmark.util.NexmarkUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class NEXMarkEventGeneratorFactory extends EventGeneratorFactory {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(NEXMarkEventGeneratorFactory.class);

    @Override
    public void setDataGenerationConfiguration(Map<String, Object> dataGenerationConfiguration) {
        NexmarkConfiguration configParameters;
        configParameters = NexmarkUtils.MAPPER.convertValue(dataGenerationConfiguration, NexmarkConfiguration.class);

        // update parameters that are passed via CLI args
        configParameters.numEventGenerators = getNumberOfGenerators();

        LOGGER.info("Configuration:\n" + configParameters);

        long basetimeOffset = 10_000;
        long basetime = System.currentTimeMillis();

        GeneratorConfig generatorConfig =
                new GeneratorConfig(configParameters, basetime, 0, configParameters.numEvents, 0);
        List<GeneratorConfig> generatorConfigs = generatorConfig.split(getNumberOfGenerators());

        basetime += basetimeOffset;

        for (GeneratorConfig config:
                generatorConfigs) {
            getEventGenerators().add(new NEXMarkEventGenerator(config, 0, basetime, configParameters.isRateLimited, getNumberOfGenerators()));
        }
    }

}
