package net.michaelkoepf.spegauge.driver.collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Array;
import java.util.concurrent.atomic.AtomicInteger;

public class InMemoryAppendOnlyLog<T> implements EventContainer<T> {

  private static final Logger LOGGER = LoggerFactory.getLogger(InMemoryAppendOnlyLog.class);
  private final T[] log;
  private final AtomicInteger lastElementIndex = new AtomicInteger(0);
  private volatile boolean poisonPill = false;

  public InMemoryAppendOnlyLog(Class<T> type, int capacity) {
    @SuppressWarnings("unchecked")
    final T[] log = (T[]) Array.newInstance(type, capacity);

    LOGGER.info("Created InMemoryAppendOnlyLog with capacity: " + capacity);

    this.log = log;
  }

  @Override
  public void add(T t) {
    if (poisonPill) {
      throw new RuntimeException("Poison pill activated");
    }

    int currIdx = lastElementIndex.get();
    this.log[currIdx] = t;
    lastElementIndex.set(currIdx + 1);
  }

  @Override
  public T get(int index) {
    if (poisonPill) {
      throw new RuntimeException("Poison pill activated");
    }

    if (index >= lastElementIndex.get()) {
      return null;
    }

    return this.log[index];
  }

  @Override
  public T get() {
    throw new UnsupportedOperationException("Not supported for append only log");
  }

  @Override
  public int size() {
    return lastElementIndex.get();
  }

  @Override
  public boolean isEmpty() {
    return lastElementIndex.get() == 0;
  }

  @Override
  public boolean isBounded() {
    return true;
  }

  @Override
  public boolean isBlocking() {
    return false;
  }

  @Override
  public int capacity() {
    return log.length;
  }

  @Override
  public int totalEvents() {
    return log.length;
  }

  @Override
  public boolean accessibleByIndex() {
    return true;
  }

  @Override
  public void clear() {
    LOGGER.warn("Clearing an append-only log is not supported");
  }

  @Override
  public boolean addAll(EventContainer<T> other) {
    throw new UnsupportedOperationException("Not supported for append only log");
  }

  @Override
  public void activatePoisonPill() {
    poisonPill = true;
  }

  @Override
  public boolean isAppendOnly() {
    return true;
  }
}
