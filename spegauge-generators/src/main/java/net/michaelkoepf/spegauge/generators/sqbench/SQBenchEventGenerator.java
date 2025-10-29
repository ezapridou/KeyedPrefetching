package net.michaelkoepf.spegauge.generators.sqbench;

import net.michaelkoepf.spegauge.api.common.model.sqbench.EntityRecordFull;
import net.michaelkoepf.spegauge.api.driver.Event;
import net.michaelkoepf.spegauge.api.driver.EventGenerator;
import net.michaelkoepf.spegauge.api.driver.RateLimitInformation;
import net.michaelkoepf.spegauge.generators.sqbench.common.SQBenchConfiguration;
import net.michaelkoepf.spegauge.generators.sqbench.datageneration.EntityGenerator;
import org.apache.commons.rng.UniformRandomProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SQBenchEventGenerator implements EventGenerator {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(SQBenchEventGenerator.class);
    private final int numGenerators;
    private final int generatorId;
    public final long firstEventId;
    public final long baseTime;
    private final EntityGenerator entityGenerator;
    private final long numEventsPerGenerator;
    public long currentEventCount = 0;
    private final int numOfQueries;


    public SQBenchEventGenerator(long baseTime, SQBenchConfiguration configuration, int generatorId, int numGenerators, UniformRandomProvider rng) {
        this.firstEventId = generatorId; //(configuration.numEventsTotal/numGenerators) * generatorId;
        this.baseTime = baseTime;
        this.numOfQueries = configuration.numOfQueries;

        this.entityGenerator = new EntityGenerator(configuration.scenarios, numGenerators, rng, numOfQueries);
        this.entityGenerator.setFirstScenario();

        this.generatorId = generatorId;
        this.numGenerators = numGenerators;

        // Last generator gets remaining events
        this.numEventsPerGenerator = (configuration.numEventsTotal/numGenerators) + (generatorId == (numGenerators - 1) ? configuration.numEventsTotal % numGenerators : 0);
    }

    /**
     * Copy constructor.
     *
     * @param toBeCloned The event generator to be cloned.
     */
    private SQBenchEventGenerator(SQBenchEventGenerator toBeCloned) {
        this.firstEventId = toBeCloned.firstEventId;
        this.baseTime = toBeCloned.baseTime;
        this.entityGenerator = toBeCloned.entityGenerator.copy();
        this.generatorId = toBeCloned.generatorId;
        this.numGenerators = toBeCloned.numGenerators;
        this.numEventsPerGenerator = toBeCloned.numEventsPerGenerator;
        this.currentEventCount = toBeCloned.currentEventCount;
        this.numOfQueries = toBeCloned.numOfQueries;
    }

    @Override
    public boolean isRateLimited() {
        return entityGenerator.isRateLimited();
    }

    @Override
    public RateLimitInformation getRateLimitInformation() {
        return entityGenerator.getRateLimitInformation();
    }

    @Override
    public boolean hasNext() {
        return currentEventCount < numEventsPerGenerator;
    }

    @Override
    public Event next() {
        long nextEventId = firstEventId + (currentEventCount * numGenerators);//firstEventId + currentEventCount;
        long eventTimeOffsetMilliSeconds = Math.floorDiv(Math.addExact(Math.floorDiv(Math.multiplyExact(generatorId, entityGenerator.getInterEventDelayNanoSeconds()), numGenerators), Math.multiplyExact(entityGenerator.getInterEventDelayNanoSeconds(), currentEventCount)), 1_000_000L);
        long eventTimeStampMilliSeconds = Math.addExact(baseTime, eventTimeOffsetMilliSeconds);

        EntityRecordFull entity = entityGenerator.getNextEntityRecord(nextEventId, eventTimeStampMilliSeconds);
        entityGenerator.update(currentEventCount);

        currentEventCount++;

        if (isRateLimited()) {
            return new net.michaelkoepf.spegauge.generators.sqbench.SQBenchEvent(entity.wallClockTimeStampMilliSecondsSinceEpoch, entity.eventTimeStampMilliSecondsSinceEpoch, entity, entityGenerator.getRateLimitInformation().getEventsPerSecond());
        } else {
            return new net.michaelkoepf.spegauge.generators.sqbench.SQBenchEvent(entity.wallClockTimeStampMilliSecondsSinceEpoch, entity.eventTimeStampMilliSecondsSinceEpoch, entity);
        }
    }
}
