package net.michaelkoepf.spegauge.driver.collection;

public interface EventContainer<T> {
    void add(T event);
    T get();
    T get(int index);
    int size();
    boolean isEmpty();
    boolean isBounded();
    boolean isBlocking();
    boolean accessibleByIndex();
    default int capacity() {
        return Integer.MAX_VALUE;
    }
    void clear();
    boolean addAll(EventContainer<T> other);
    int totalEvents();

    void activatePoisonPill();

    default boolean isAppendOnly() {
        return false;
    }
}
