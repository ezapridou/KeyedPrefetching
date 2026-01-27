package net.michaelkoepf.spegauge.driver.collection;

import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class EventArrayBlockingQueue<T> implements EventContainer<T> {

  private final BlockingQueue<T> queue;
  private final int capacity;
  private final int totalEvents;

  public EventArrayBlockingQueue(int capacity, int totalEvents) {
    this.capacity = capacity;
    this.queue = new ArrayBlockingQueue<>(capacity);
    this.totalEvents = totalEvents;
  }

  @Override
  public void add(T event) {
    try {
      this.queue.put(event);
    } catch (InterruptedException e) {
      throw new UncheckedInterruptedException(e);
    }
  }

  @Override
  public T get(int index) {
    throw new UnsupportedOperationException("Not supported for blocking queue");
  }

  @Override
  public T get() {
    try {
      return queue.take();
    } catch (InterruptedException e) {
      throw new UncheckedInterruptedException(e);
    }
  }

  @Override
  public int size() {
    return this.queue.size();
  }

  @Override
  public boolean isEmpty() {
    return this.queue.isEmpty();
  }

  @Override
  public boolean isBounded() {
    return true;
  }

  @Override
  public boolean isBlocking() {
    return true;
  }

  @Override
  public int capacity() {
    return this.capacity;
  }

  @Override
  public boolean accessibleByIndex() {
    return false;
  }

  @Override
  public void clear() {
    this.queue.clear();
  }

  @Override
  public boolean addAll(EventContainer<T> other) {
    return this.queue.addAll((Collection<? extends T>) other);
  }

  @Override
  public int totalEvents() {
    return totalEvents;
  }

  @Override
  public void activatePoisonPill() {
    throw new UnsupportedOperationException("Not supported for blocking queue");
  }
}
