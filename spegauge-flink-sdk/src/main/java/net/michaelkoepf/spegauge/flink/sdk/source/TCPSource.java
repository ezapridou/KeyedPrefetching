package net.michaelkoepf.spegauge.flink.sdk.source;

import net.michaelkoepf.spegauge.api.QueryConfig;
import net.michaelkoepf.spegauge.flink.sdk.source.impl.SQBenchParallelTCPSourceFull;
import net.michaelkoepf.spegauge.flink.sdk.source.impl.SQBenchParallelTCPSourceNexmarkJSON;
import net.michaelkoepf.spegauge.flink.sdk.source.impl.SQBenchParallelTCPSourceNoExtraAttrs;
import net.michaelkoepf.spegauge.flink.sdk.source.impl.SQBenchParallelTCPSourceYSBJSON;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;

public class TCPSource<T> implements Source<T, TCPSourceSplit, Void> {

    private final String hostname;
    private final int port;
    private final boolean fullRecords;
    private final boolean markersOn;

    public TCPSource(String hostname, int port, boolean markersOn, boolean fullRecords) {
        this.hostname = hostname;
        this.port = port;
        this.fullRecords = fullRecords;
        this.markersOn = markersOn;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<T, TCPSourceSplit> createReader(SourceReaderContext context) {
        SocketReader<T> socketReader;
        if (QueryConfig.SELECTED_BENCHMARK == QueryConfig.BENCHMARK_TYPE.SQBENCH_DEFAULT) {
            if (fullRecords) {
                socketReader = (SocketReader<T>) new SQBenchParallelTCPSourceFull();
            } else {
                socketReader = (SocketReader<T>) new SQBenchParallelTCPSourceNoExtraAttrs();
            }
        } else if (QueryConfig.SELECTED_BENCHMARK == QueryConfig.BENCHMARK_TYPE.NEXMARK){
            socketReader = (SocketReader<T>) new SQBenchParallelTCPSourceNexmarkJSON(markersOn);
        } else if (QueryConfig.SELECTED_BENCHMARK == QueryConfig.BENCHMARK_TYPE.YSB){
            socketReader = (SocketReader<T>) new SQBenchParallelTCPSourceYSBJSON();
        } else {
            throw new UnsupportedOperationException("Unsupported benchmark type: " + QueryConfig.SELECTED_BENCHMARK);
        }
        return new TCPSourceReader<>(context, socketReader);
    }

    @Override
    public SplitEnumerator<TCPSourceSplit, Void> createEnumerator(SplitEnumeratorContext<TCPSourceSplit> context) {
        return new TCPSourceEnumerator(context, hostname, port);
    }

    @Override
    public SplitEnumerator<TCPSourceSplit, Void> restoreEnumerator(SplitEnumeratorContext<TCPSourceSplit> context, Void checkpoint) {
        return createEnumerator(context);
    }

    @Override
    public SimpleVersionedSerializer<TCPSourceSplit> getSplitSerializer() {
        return new TCPSourceSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<Void> getEnumeratorCheckpointSerializer() {
        // Just a dummy serializer
        return new SimpleVersionedSerializer<Void>() {
            @Override
            public int getVersion() {
                return 1;
            }

            @Override
            public byte[] serialize(Void obj) {
                return new byte[0]; // nothing to serialize
            }

            @Override
            public Void deserialize(int version, byte[] serialized) {
                return null; // stateless restore
            }
        };
    }
}

