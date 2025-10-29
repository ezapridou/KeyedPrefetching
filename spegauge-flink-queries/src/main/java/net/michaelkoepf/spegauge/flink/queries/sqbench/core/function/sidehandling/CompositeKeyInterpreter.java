package net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.sidehandling;

// Composite keys: uses *your* encoding (1 for FIRST, 0 for SECOND)
public final class CompositeKeyInterpreter implements KeyInterpreter<Long> {
    public Long joinKey(Long k) { return k >>> 1; } // unsigned shift
    public StreamSide sideOf(Long k) { return ((k & 1L) == 1L) ? StreamSide.FIRST : StreamSide.SECOND; }
    public Long encode(Long joinKey, StreamSide s) {
        return SideHandle.mergedKeyStateId(joinKey, s);
    }
}
