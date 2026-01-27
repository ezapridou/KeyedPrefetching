package net.michaelkoepf.spegauge.flink.sdk.source;

import lombok.Getter;
import lombok.Setter;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import java.io.*;
import java.net.ProtocolException;
import java.net.Socket;

public abstract class SocketReader<T> {
    private volatile boolean running = true;

    @Getter
    private static final Logger LOGGER =
            LoggerFactory.getLogger(SocketReader.class);
    protected static final Marker KEYED_PREFETCHING = MarkerFactory.getMarker("KeyedPrefetching");

    @Getter
    @Setter
    private String delimiter = "\n";
    @Getter
    @Setter
    private int readBufferSize = 8192;

    private final char[] readBuffer = new char[readBufferSize];
    private final StringBuilder buffer = new StringBuilder();

    /**
     * Specifies the time semantics of the source.
     */

    private Socket socket;
    private BufferedReader socketReader;

    @Getter
    private Writer socketWriter;
    @Getter
    String driverJobId;
    @Getter
    int driverSubTaskIdx;

    @Getter
    private long eventCount = 0;

    @Getter
    private volatile boolean cancelled = false;

    private SourceReaderContext context;

    public void start(SourceReaderContext context) {
        this.context = context;
    }

    /**
     * Converts the string given by line to the desired event object, if the string encodes an event.
     *
     * @param record string representation of event object
     * @return event object if record contained an event, null otherwise (e.g., in case of control messages)
     */
    public abstract T getEvent(String record);

    public abstract T getMarker(long eventCount);

    public void setupConnection(TCPSourceSplit split) {
        LOGGER.debug(KEYED_PREFETCHING, "Connecting to host " + split.getHostname() + " on port " + split.getPort());
        try {
            socket = new Socket(split.getHostname(), split.getPort());
            socket.setTcpNoDelay(true); // source sends only control messages to driver, no need to buffer
            socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            socketWriter = new OutputStreamWriter(socket.getOutputStream());

            LOGGER.debug(KEYED_PREFETCHING, "Waiting for DRIVER HELLO message");

            String driverHello = socketReader.readLine();

            if (driverHello.startsWith("DRIVER HELLO ")) {
                String params = driverHello.replaceFirst("^DRIVER HELLO ", "");
                String[] jobId_subtaskIdx = params.split(" ");
                driverJobId = jobId_subtaskIdx[0];
                driverSubTaskIdx = Integer.parseInt(jobId_subtaskIdx[1]);
            } else {
                throw new ProtocolException("Expected DRIVER HELLO message from driver, but got: " + driverHello);
            }

            LOGGER.debug(KEYED_PREFETCHING, "Received DRIVER HELLO AND CONNECTED SUCCESSFULLY to " + split.getHostname()
                    + " on port " + split.getPort());

        } catch (IOException e) {
            throw new RuntimeException("Failed to connect to TCP source", e);
        }
    }

    public InputStatus poll(ReaderOutput<T> output) {
        if (!running || socketReader == null) return InputStatus.NOTHING_AVAILABLE;

        try {
            int charsRead = socketReader.read(readBuffer);
            if (charsRead == -1) return InputStatus.END_OF_INPUT;

            buffer.append(readBuffer, 0, charsRead);

            int currentDelimiterPos;
            while (buffer.length() >= this.delimiter.length() && (currentDelimiterPos = buffer.indexOf(this.delimiter)) != -1) {
                String record = buffer.substring(0, currentDelimiterPos).replace("\r", "");

                T event = getEvent(record);

                // not a regular event (e.g., control message)
                if (event == null) {
                    buffer.delete(0, currentDelimiterPos + this.delimiter.length());
                    continue;
                }

                output.collect(event);

                T marker = getMarker(eventCount);
                if (marker != null){
                    output.collect(marker);
                }

                eventCount++;
                buffer.delete(0, currentDelimiterPos + this.delimiter.length());
            }

            return InputStatus.MORE_AVAILABLE;
        } catch (IOException e) {
            LOGGER.error("An error occurred while communicating with the server: " + e.getMessage());
            throw new UncheckedIOException(e);
        }
    }


    public void close() {
        running = false;
        try {
            if (socketWriter != null) {
                socketWriter.flush();
                socketWriter.close();
            }
            if (socketReader != null) {
                socketReader.close();
            }
            if (socket != null && !socket.isClosed()){
                socket.close();
            }
        } catch (IOException ignored) {
            throw new RuntimeException("Failed to close TCP source");
        }
    }
}

