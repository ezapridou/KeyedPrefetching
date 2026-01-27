package net.michaelkoepf.spegauge.flink.sdk.source;

import org.apache.flink.api.connector.source.SourceSplit;

public class TCPSourceSplit implements SourceSplit {
    private final String splitId;
    private final String hostname;
    private final int port;

    public TCPSourceSplit(String splitId, String hostname, int port) {
        this.splitId = splitId;
        this.hostname = hostname;
        this.port = port;
    }

    @Override
    public String splitId() {
        return splitId;
    }

    public String getHostname() {
        return hostname;
    }

    public int getPort() {
        return port;
    }
}