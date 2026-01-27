package net.michaelkoepf.spegauge.flink.sdk.sink;

import net.michaelkoepf.spegauge.api.common.ObservableEvent;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Instant;

public class EventTimeLatencyMeter extends RichMapFunction<ObservableEvent, ObservableEvent> {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventTimeLatencyMeter.class);

    private long lastSampleTime;
    private DatagramSocket socket;
    private final long sampleIntervalMilliSeconds;
    private InetAddress hostname = null;
    private int udpPort;
    private String messagePrefix;

    public EventTimeLatencyMeter(long sampleIntervalMilliSeconds) throws UnknownHostException {
        if (sampleIntervalMilliSeconds <= 0) {
            throw new IllegalArgumentException("Sample interval must be greater than 0");
        }

        this.sampleIntervalMilliSeconds = sampleIntervalMilliSeconds;
        this.lastSampleTime = Instant.now().toEpochMilli();
    }

    public EventTimeLatencyMeter(String hostname, int udpPort, long sampleIntervalMilliSeconds) throws UnknownHostException {
        this.hostname = InetAddress.getByName(hostname);
        this.udpPort = udpPort;

        if (sampleIntervalMilliSeconds <= 0) {
            throw new IllegalArgumentException("Sample interval must be greater than 0");
        }

        this.sampleIntervalMilliSeconds = sampleIntervalMilliSeconds;
        this.lastSampleTime = Instant.now().toEpochMilli();
    }

    @Override
    public void open(OpenContext parameters) throws Exception {
        super.open(parameters);
        socket = new DatagramSocket();
        messagePrefix = getRuntimeContext().getJobInfo().getJobId() + " "
                + getRuntimeContext().getTaskInfo().getIndexOfThisSubtask() + ":";
    }

    @Override
    public ObservableEvent map(ObservableEvent value) throws Exception {
        if (Instant.now().toEpochMilli() >= (lastSampleTime + sampleIntervalMilliSeconds)) {
            long eventTimeLatency = Instant.now().toEpochMilli() - value.getEventEmissionTimeStampMilliSeconds();

            String message = messagePrefix + eventTimeLatency + "\n";

            if (hostname == null) {
               LOGGER.info(message);
            } else {
                DatagramPacket packet = new DatagramPacket(message.getBytes(), message.length(), hostname, udpPort);
                socket.send(packet);
            }
            lastSampleTime = Instant.now().toEpochMilli();
        }
        return value;
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (socket != null && !socket.isClosed()) {
            socket.close();
        }
    }
}
