package net.michaelkoepf.spegauge.driver.core;

import net.michaelkoepf.spegauge.api.driver.LoadShedder;
import net.michaelkoepf.spegauge.api.driver.SheddableEvent;

/**
 * A dummy load shedder that never sheds any events.
 */
public class DummyLoadShedder implements LoadShedder {

  public DummyLoadShedder() {
  }

  @Override
  public boolean shed(SheddableEvent event) {
    return false;
  }

  @Override
  public void step() {
  }

  @Override
  public LoadShedder copy() {
    return new DummyLoadShedder();
  }
}
