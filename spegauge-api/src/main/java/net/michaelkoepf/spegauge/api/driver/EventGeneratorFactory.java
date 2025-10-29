package net.michaelkoepf.spegauge.api.driver;

import lombok.Getter;
import lombok.Setter;

import java.util.*;

/**
 * Factory to initialize and create EventGenerators.
 */
// TODO: make iterable?
public abstract class EventGeneratorFactory implements Iterator<EventGenerator> {

  @Getter
  private int numberOfGenerators;

  @Getter
  private boolean initialized = false;

  @Getter
  private final List<EventGenerator> eventGenerators = new LinkedList<>();

  private int currentGenerator = 0;

  /**
   * Initializes the factory and sets the number of generators to be created. This method needs to be invoked before any other methods of this and the extended interfaces.
   *
   * @param numberOfGenerators number of generators
   */
  public void initialize(int numberOfGenerators) {
    this.numberOfGenerators = numberOfGenerators;
    this.initialized = true;
  }

  /**
   * Sets the data generation configuration (if needed). If this method is called, it must be called before the first call to getEventGenerator.
   *
   * @param dataGenerationConfiguration A map of configuration parameters.
   * @throws IllegalStateException If the factory is not initialized.
   * @throws IllegalArgumentException If the configuration is invalid.
   */
  public abstract void setDataGenerationConfiguration(Map<String, Object> dataGenerationConfiguration);

  /**
   * Like {@link Iterator#hasNext()} but with a stronger post condition.
   *
   * @return True if there are more EventGenerators.
   * @throws IllegalStateException If the factory is not initialized.
   */
  @Override
  public final boolean hasNext() {
    if (!initialized) {
      throw new IllegalStateException("Factory not initialized");
    }

    return currentGenerator < eventGenerators.size();
  }

  /**
   * Like {@link Iterator#next()}, but with a stronger post condition.
   *
   * @return The next EventGenerator.
   * @throws IllegalStateException If the factory is not initialized.
   * @throws NoSuchElementException If there are no more EventGenerators.
   */
  @Override
  public final EventGenerator next() {
    if (!initialized) {
      throw new IllegalStateException("Factory not initialized");
    }

    if (hasNext()) {
      EventGenerator eventGenerator =  eventGenerators.get(currentGenerator);
      currentGenerator++;
      return eventGenerator;
    } else {
      throw new NoSuchElementException("No more event generators");
    }
  }

}
