package net.michaelkoepf.spegauge.flink.sdk.source;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.*;

public class TCPSourceSplitSerializer implements SimpleVersionedSerializer<TCPSourceSplit> {

    private static final int VERSION = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(TCPSourceSplit split) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        dos.writeUTF(split.splitId());
        dos.writeUTF(split.getHostname());
        dos.writeInt(split.getPort());

        dos.flush();
        return baos.toByteArray();
    }

    @Override
    public TCPSourceSplit deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION) {
            throw new IOException("Unsupported version: " + version);
        }

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(serialized));

        String splitId = dis.readUTF();
        String hostname = dis.readUTF();
        int port = dis.readInt();

        return new TCPSourceSplit(splitId, hostname, port);
    }
}

