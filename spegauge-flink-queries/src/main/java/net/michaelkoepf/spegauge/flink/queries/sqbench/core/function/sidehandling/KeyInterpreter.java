package net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.sidehandling;

public interface KeyInterpreter<K> {
    K joinKey(K k);
    StreamSide sideOf(K k);               // For single-input, always StreamSide.FIRST
    K encode(K joinKey, StreamSide s); // For single-input, returns joinKey
}
