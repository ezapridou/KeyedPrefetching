package net.michaelkoepf.spegauge.api.driver;

import org.apache.commons.rng.simple.RandomSource;

import java.util.Set;

/**
 * Configuration information of the JAR file containing event generators.
 */
public interface Config {

    /**
     * Returns the number of event generator factories that are exposed by this JAR file.
     *
     * @return The number of event generators.
     */
    int getNumberOfEventGeneratorFactories();

    /**
     * Returns the fully qualified names of event generator factories that are exposed by this JAR file.
     *
     * @return The fully qualified names of event generator factories.
     */
    Set<String> getEventGeneratorFactoriesFullyQualifiedNames();

    /**
     * Returns the default random source to be used by the event generators.
     *
     * @return The default random source.
     */
    RandomSource getDefaultRandomSource();

}
