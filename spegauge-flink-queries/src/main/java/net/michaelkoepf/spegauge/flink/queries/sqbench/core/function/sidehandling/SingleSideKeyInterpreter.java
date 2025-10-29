package net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.sidehandling;

// Single-input default: key == joinKey, side==FIRST
public final class SingleSideKeyInterpreter<T> implements KeyInterpreter<T> {
    public T joinKey(T k) { return k; }
    public StreamSide sideOf(T k) { return StreamSide.FIRST; }
    public T encode(T joinKey, StreamSide s) { return joinKey; }
}
