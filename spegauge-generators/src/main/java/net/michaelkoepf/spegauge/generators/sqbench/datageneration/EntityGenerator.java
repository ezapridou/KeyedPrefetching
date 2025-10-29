package net.michaelkoepf.spegauge.generators.sqbench.datageneration;

import lombok.Getter;
import net.michaelkoepf.spegauge.api.common.model.sqbench.EntityRecordFull;
import net.michaelkoepf.spegauge.api.driver.RateLimitInformation;
import net.michaelkoepf.spegauge.generators.Config;
import net.michaelkoepf.spegauge.generators.sqbench.SQBenchEventGenerator;
import net.michaelkoepf.spegauge.generators.sqbench.common.SQBenchConfiguration;
import org.apache.commons.rng.RandomProviderState;
import org.apache.commons.rng.RestorableUniformRandomProvider;
import org.apache.commons.rng.UniformRandomProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Wrapper around multiple EntityRecordGenerator instances that also keeps tracks of the current scenario.
 */
public final class EntityGenerator {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(SQBenchEventGenerator.class);

    private static final net.michaelkoepf.spegauge.api.driver.Config CONFIG = new Config();

    // linked hashmap ensures that order of creation is preserved when cloning
    private final LinkedHashMap<EntityRecordFull.Type, EntityRecordGenerator> entityGenerators = new LinkedHashMap<>();
    private final int numGenerators;
    private final int numOfQueries;
    private final List<SQBenchConfiguration.Scenario> scenarios = new ArrayList<>();
    private final List<Long> nextScenarioStartEventNumber = new ArrayList<>();
    private final UniformRandomProvider rng;
    private boolean initialized = false;

    // TODO: you may add further entities here

    @Getter
    private long proportionEntityA;

    @Getter
    private long proportionEntityB;

    @Getter
    private long proportionEntityC;

    @Getter
    private long totalProportion;

    @Getter
    private long proportionScalingFactor;

    @Getter
    private RateLimitInformation rateLimitInformation;

    @Getter
    private long interEventDelayNanoSeconds;

    @Getter
    private boolean isRateLimited;

    private SQBenchConfiguration.Scenario activeScenario;

    public EntityGenerator(List<SQBenchConfiguration.Scenario> scenarios, int numGenerators, UniformRandomProvider rng, int numOfQueries) {
        this.numGenerators = numGenerators;
        this.numOfQueries = numOfQueries;
        this.scenarios.addAll(scenarios);

        if (scenarios.isEmpty()) {
            throw new IllegalArgumentException("At least one scenario must be given");
        }

        long lastEventNumber = 0;
        for (int i = 0; i < this.scenarios.size() - 1; i++) {
            long nextScenarioAt = (this.scenarios.get(i).nextScenarioAfter/numGenerators) + (i == (numGenerators - 1) ? this.scenarios.get(i).nextScenarioAfter % numGenerators : 0) + lastEventNumber;

            if (nextScenarioAt <= lastEventNumber) {
                throw new IllegalArgumentException("lastEventNumber must be strictly increasing");
            }

            nextScenarioStartEventNumber.add(nextScenarioAt);
        }

        this.rng = rng;
    }

    private EntityGenerator(EntityGenerator toBeCloned) {
        if (toBeCloned == null) {
            throw new IllegalArgumentException("Object to be cloned must not be null");
        }

        if (!toBeCloned.initialized) {
            throw new IllegalArgumentException("Object to be cloned must be initialized");
        }

        this.numGenerators = toBeCloned.numGenerators;
        this.numOfQueries = toBeCloned.numOfQueries;
        this.scenarios.addAll(toBeCloned.scenarios);
        this.nextScenarioStartEventNumber.addAll(toBeCloned.nextScenarioStartEventNumber);

        RandomProviderState randomProviderState = ((RestorableUniformRandomProvider) toBeCloned.rng).saveState();
        this.rng = CONFIG.getDefaultRandomSource().create();
        ((RestorableUniformRandomProvider) this.rng).restoreState(randomProviderState);

        for (Map.Entry<EntityRecordFull.Type, EntityRecordGenerator> entry : toBeCloned.entityGenerators.entrySet()) {
            var key = entry.getKey();
            var value = entry.getValue();
            this.entityGenerators.put(key, EntityRecordGenerator.copy(value, rng));
        }

        setActiveScenario(toBeCloned.activeScenario);

        this.initialized = true;
    }

    public EntityGenerator copy() {
        return new EntityGenerator(this);
    }

    public void setFirstScenario() {
        LOGGER.info("Setting first scenario (" + this.scenarios.get(0).scenarioName + "). " + (!this.nextScenarioStartEventNumber.isEmpty() ? "Next scenario change will be at event number " + this.nextScenarioStartEventNumber.get(0) : "There will be no scenario changes."));
        SQBenchConfiguration.Scenario scenario = this.scenarios.remove(0);

        // create entity generators
        entityGenerators.put(scenario.entityA.entityType, new EntityRecordGenerator(rng, scenario.entityA, numOfQueries));
        entityGenerators.put(scenario.entityB.entityType, new EntityRecordGenerator(rng, scenario.entityB, numOfQueries));
        entityGenerators.put(scenario.entityC.entityType, new EntityRecordGenerator(rng, scenario.entityC, numOfQueries));
        // TODO: you ma add further entities here

        setActiveScenario(scenario);

        initialized = true;
    }

    public void update(long currentEventCount) {
        if (!initialized) {
            throw new IllegalStateException("ScenarioData not initialized. Before calling this method, call setFirstScenario() first");
        }

        if (!this.nextScenarioStartEventNumber.isEmpty() && this.nextScenarioStartEventNumber.get(0) == (currentEventCount + 1)) {
            LOGGER.info("Setting next scenario  (" + this.scenarios.get(0).scenarioName + "). Event number in current generator (zero-based): " + currentEventCount + ". " + (this.nextScenarioStartEventNumber.size() > 1 ? "Next scenario change will be at event number " + this.nextScenarioStartEventNumber.get(1) : "This was the last scenario change."));
            this.nextScenarioStartEventNumber.remove(0);
            SQBenchConfiguration.Scenario scenario = this.scenarios.remove(0);
            setActiveScenario(scenario);
        }
    }

    public EntityRecordFull getNextEntityRecord(long nextEventId, long eventTimeStamp) {
        long rem = nextEventId % (this.totalProportion * this.proportionScalingFactor);

        try {
            // TODO: you ma add further entities here
            if (rem < (this.proportionEntityA * this.proportionScalingFactor)) {
                return entityGenerators.get(EntityRecordFull.Type.A).nextEntity(nextEventId, eventTimeStamp);
            } else if (rem < ((this.proportionEntityA + this.proportionEntityB) * this.proportionScalingFactor)) {
                return entityGenerators.get(EntityRecordFull.Type.B).nextEntity(nextEventId, eventTimeStamp);
            } else if (rem < ((this.proportionEntityA + this.proportionEntityB + this.proportionEntityC) * this.proportionScalingFactor)) {
                return entityGenerators.get(EntityRecordFull.Type.C).nextEntity(nextEventId, eventTimeStamp);
            } else {
                throw new IllegalStateException("Not implemented");
            }
        } catch (Exception e) {
            LOGGER.error("Error while generating next entity with JSON payload", e);
            throw new RuntimeException(e);
        }
    }

    private void setActiveScenario(SQBenchConfiguration.Scenario activeScenario) {
        setActiveScenarioObjectVariables(activeScenario);

        // update entity generators
        entityGenerators.get(activeScenario.entityA.entityType).setAttributes(activeScenario.entityA, activeScenario);
        entityGenerators.get(activeScenario.entityB.entityType).setAttributes(activeScenario.entityB, activeScenario);
        entityGenerators.get(activeScenario.entityC.entityType).setAttributes(activeScenario.entityC, activeScenario);
        // TODO: you may add further entities here

        // update FK relationships
        for (Map.Entry<EntityRecordFull.Type, EntityRecordGenerator> entry : entityGenerators.entrySet()) {
            var currentEntity = entry.getValue();

            EntityRecordFull.Type referencedEntity = currentEntity.getReferencedEntityType();

            if (referencedEntity != null) {
                long[] PKs = entityGenerators.get(referencedEntity).getPKs();
                currentEntity.setFKs(PKs);
            }
        }
    }

    private void setActiveScenarioObjectVariables(SQBenchConfiguration.Scenario activeScenario) {
        this.activeScenario = activeScenario;

        // TODO: you may add further entities here
        this.proportionEntityA = activeScenario.proportionEntityA;
        this.proportionEntityB = activeScenario.proportionEntityB;
        this.proportionEntityC = activeScenario.proportionEntityC;

        this.totalProportion = this.proportionEntityA + this.proportionEntityB + this.proportionEntityC;
        this.proportionScalingFactor = activeScenario.proportionScalingFactor;

        this.isRateLimited = activeScenario.isRateLimited;

        this.rateLimitInformation = new RateLimitInformation(activeScenario.numEventsPerSecond, this.numGenerators);
        this.interEventDelayNanoSeconds = (long) ((1_000_000_000.0 / activeScenario.numEventsPerSecond) * this.numGenerators);

        if (interEventDelayNanoSeconds == 0) {
            throw new IllegalArgumentException("Number of events per second too high for number of generators");
        }
    }
}
