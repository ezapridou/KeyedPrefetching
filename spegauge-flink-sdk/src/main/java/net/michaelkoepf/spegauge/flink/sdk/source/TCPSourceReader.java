package net.michaelkoepf.spegauge.flink.sdk.source;

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue.AVAILABLE;

public class TCPSourceReader<T> implements SourceReader<T, TCPSourceSplit> {

    private final SourceReaderContext context;
    private final SocketReader<T> socketReader;

    public TCPSourceReader(SourceReaderContext context, SocketReader<T> socketReader) {
        this.context = context;
        this.socketReader = socketReader;
        socketReader.start(context);
    }

    @Override
    public void start() {
        context.sendSplitRequest();
    }

    @Override
    public InputStatus pollNext(ReaderOutput<T> output) throws Exception {
        return socketReader.poll(output);
    }

    @Override
    public List<TCPSourceSplit> snapshotState(long checkpointId) {
        return Collections.emptyList(); // stateless for now
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        return AVAILABLE;
    }

    @Override
    public void addSplits(List<TCPSourceSplit> splits) {
        for (TCPSourceSplit split : splits) {
            socketReader.setupConnection(split);
        }
    }

    @Override
    public void notifyNoMoreSplits() {}

    @Override
    public void close() throws Exception {
        socketReader.close();
    }

    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {}

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}
}

