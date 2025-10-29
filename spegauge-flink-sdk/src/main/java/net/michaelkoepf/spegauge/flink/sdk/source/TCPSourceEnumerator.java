package net.michaelkoepf.spegauge.flink.sdk.source;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import javax.annotation.Nullable;
import java.util.List;

public class TCPSourceEnumerator implements SplitEnumerator<TCPSourceSplit, Void> {

    private final SplitEnumeratorContext<TCPSourceSplit> context;
    private final String hostname;
    private final int port;

    public TCPSourceEnumerator(SplitEnumeratorContext<TCPSourceSplit> context, String hostname, int port) {
        this.context = context;
        this.hostname = hostname;
        this.port = port;
    }

    @Override
    public void start() {
        // We assign splits on-demand when readers request them
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        TCPSourceSplit split = new TCPSourceSplit("tcp-split-" + subtaskId, hostname, port);
        context.assignSplit(split, subtaskId);
    }

    @Override
    public void addSplitsBack(List<TCPSourceSplit> splits, int subtaskId) {
        for (TCPSourceSplit split : splits) {
            context.assignSplit(split, subtaskId);
        }
    }

    @Override
    public void addReader(int subtaskId) {
        // do nothing
    }

    @Override
    public Void snapshotState(long checkpointId) {
        // do nothing
        return null;
    }

    @Override
    public void close() {}
}

