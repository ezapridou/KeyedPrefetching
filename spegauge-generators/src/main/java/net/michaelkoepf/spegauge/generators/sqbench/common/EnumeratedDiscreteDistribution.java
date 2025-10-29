package net.michaelkoepf.spegauge.generators.sqbench.common;

import org.apache.commons.rng.UniformRandomProvider;
import org.apache.commons.statistics.distribution.DiscreteDistribution;

/**
 * A discrete distribution that samples from a fixed set of outcomes (0-9) with given probabilities.
 */
public class EnumeratedDiscreteDistribution implements DiscreteDistribution.Sampler {

  private static final int[] OUTCOMES = new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
  private final double[] probabilities;
  private final UniformRandomProvider rng;

  /**
   * Create a new instance.
   *
   * @param probabilities the probabilities of each outcome (length 10)
   * @param rng the random number generator
   */
  public EnumeratedDiscreteDistribution(double[] probabilities, UniformRandomProvider rng) {
    this.probabilities = probabilities;
    this.rng = rng;
  }

  @Override
  public int sample() {
    double rand = rng.nextDouble();

    int index = -1;

    while (rand >= 0) {
      index++;
      rand -= probabilities[index];
    }

    return OUTCOMES[index];
  }
}
