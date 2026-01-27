package net.michaelkoepf.spegauge.api.sut;

import org.apache.flink.streaming.runtime.tasks.StreamTask;
import java.util.Map;

/**
 * Suggested interface for sources that can be split and merged (WIP).
 */
public interface ReconfigurableSource extends Suspendable {
    /**
     * Returns the data associated with the source.
     *
     * @return the data
     */
    ReconfigurableSourceData getData();

    void requestDisconnectionFromDriver(Map<Integer, ReconfigurableSourceData> data);

    /**
     * Modifies the connection data of the source.
     *
     * @param host the new host
     * @param port the new port
     * @throws ReconfigurationException if the connection data cannot be modified
     */
    void modifyConnectionData(String host, int port);

    void setStreamTaskPointer(StreamTask tuple);
}
