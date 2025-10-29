package net.michaelkoepf.spegauge.flink.sdk.source.impl;

import net.michaelkoepf.spegauge.flink.sdk.source.SocketReader;

import java.util.concurrent.atomic.AtomicLong;

public abstract class SQBenchParallelTCPSource<T> extends SocketReader<T> {
    protected final AtomicLong currentEventId = new AtomicLong(-1L);

    @Override
    public T getEvent(String record) {
        return handleEvent(record);
    }

    protected abstract T handleEvent(String record);
}
