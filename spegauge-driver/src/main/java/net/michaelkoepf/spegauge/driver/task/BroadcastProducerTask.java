package net.michaelkoepf.spegauge.driver.task;

import net.michaelkoepf.spegauge.api.driver.Event;
import net.michaelkoepf.spegauge.api.driver.EventGenerator;
import net.michaelkoepf.spegauge.api.driver.RateLimitInformation;
import net.michaelkoepf.spegauge.driver.collection.EventContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class BroadcastProducerTask extends AbstractTask {

  private static final Logger LOGGER = LoggerFactory.getLogger(BroadcastProducerTask.class);
  private final EventGenerator eventGenerator;
  private final RateLimitInformation rateLimitInformation;
  private AtomicInteger producedEventsSoFar;

  public BroadcastProducerTask(
          CountDownLatch readyCounter, EventContainer<Event> queue, EventGenerator eventGenerator, int subtaskIndex, long initialDelay) {
    super(readyCounter, queue, subtaskIndex, initialDelay);
    this.eventGenerator = eventGenerator;
    this.rateLimitInformation = eventGenerator.getRateLimitInformation();
  }

  @Override
  public void executeTask(EventContainer<Event> queue) {
    boolean isRateLimited = eventGenerator.isRateLimited();
    LOGGER.info("Is event generator rate limited? " + isRateLimited);

    long passedNanos = System.nanoTime();
    long passedMillis;
    long sleepMsTemp;
    int sleepNanosTemp;
    producedEventsSoFar = new AtomicInteger(0);

    LOGGER.info("Starting producing events...");

    while (eventGenerator.hasNext() && (getStatus() != Status.TERMINATED_BY_CONTROLLER)) {
      try {
        // don't generate new event if we have one from the last iteration due to interruption of put or sleep
        Event event = eventGenerator.next();

        queue.add(event);

        producedEventsSoFar.incrementAndGet();
//        throughputCounter++;
//
//        long elapsed = System.nanoTime() - lastThroughputMeasurement;
//        if (elapsed > 5_000_000_000L) {
//          LOGGER.info(("Measurement " + numMeasurements + ": ") + (long) (throughputCounter/(elapsed/1e9))+ " events/s");
//          throughputCounter = 0;
//          lastThroughputMeasurement = System.nanoTime();
//          numMeasurements++;
//        }

        if (isRateLimited) {
          if (producedEventsSoFar.get() % rateLimitInformation.getBatchSize() == 0) {
            passedNanos = System.nanoTime() - passedNanos;
            passedMillis = 0L;

            if (passedNanos >= 1000000) {
              passedMillis = passedNanos / 1000000;
              passedNanos = passedNanos % 1000000;
            }

            sleepMsTemp = rateLimitInformation.getSleepTimeMS() - passedMillis;
            sleepNanosTemp = (int) (rateLimitInformation.getSleepTimeNS() - passedNanos);

            if (sleepNanosTemp < 0) {
              sleepMsTemp = sleepMsTemp - 1;
              sleepNanosTemp = sleepNanosTemp + 1000000;
            }

            if (sleepMsTemp >= 0) {
              // no guard against spurious wakeup needed (e.g., see https://stackoverflow.com/a/2389016)
              Thread.sleep(sleepMsTemp, sleepNanosTemp); // blocking, may throw InterruptedException
            } else {
              // not needed at the moment
              //  LOGGER.warn("Rate limit exceeded. No sleep time.");
            }

            passedNanos = System.nanoTime();
          }
        }
      } catch (InterruptedException e) {
        if (getStatus() == Status.TERMINATED_BY_CONTROLLER) {
          Thread.interrupted();
          LOGGER.info("Termination requested. Stopping producer.");
          break;
        } else {
          // if an interruption occurs in any other status than the above,
          // we make the defensive decision to stop the producer
          LOGGER.error("Unexpected interruption. Current status" + getStatus() + ". Stopping producer.");
          setStatus(Status.TERMINATED_UNEXPECTEDLY);
          throw new RuntimeException(e);
        }
      }
    }

    if (!eventGenerator.hasNext()) {
      LOGGER.info("All events produced");
    } else {
      LOGGER.info("Event generator had more events.");
    }

    setStatus(Status.FINISHED);

    LOGGER.info("Producer finished");
  }

}
