package net.michaelkoepf.spegauge.generators;

import org.apache.commons.rng.simple.RandomSource;

import java.util.*;

public class Config implements net.michaelkoepf.spegauge.api.driver.Config {

    private static final Set<String> eventGeneratorFactoriesFullyQualifiedNames = Set.of("net.michaelkoepf.spegauge.generators.nexmark.NEXMarkEventGeneratorFactory", "net.michaelkoepf.spegauge.generators.sqbench.SQBenchEventGeneratorFactory");

    @Override
    public int getNumberOfEventGeneratorFactories() {
        return eventGeneratorFactoriesFullyQualifiedNames.size();
    }

    @Override
    public Set<String> getEventGeneratorFactoriesFullyQualifiedNames() {
        return eventGeneratorFactoriesFullyQualifiedNames;
    }

    @Override
    public RandomSource getDefaultRandomSource() {
        return RandomSource.XO_SHI_RO_256_PP;
    }
}
