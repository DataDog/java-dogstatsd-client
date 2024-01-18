package com.timgroup.statsd;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import jnr.unixsocket.UnixDatagramChannel;
import jnr.unixsocket.UnixSocketAddress;

import static com.timgroup.statsd.NonBlockingStatsDClient.DEFAULT_UDS_MAX_PACKET_SIZE_BYTES;

public class UnixDatagramSocketDummyStatsDServer extends DummyStatsDServer {
    private final DatagramChannel server;

    public UnixDatagramSocketDummyStatsDServer(String socketPath) throws IOException {
        server = UnixDatagramChannel.open();
        server.bind(new UnixSocketAddress(socketPath));
        this.listen();
    }

    @Override
    protected boolean isOpen() {
        return server.isOpen();
    }

    protected void receive(ByteBuffer packet) throws IOException {
        server.receive(packet);
    }

    public void close() throws IOException {
        try {
            server.close();
        } catch (Exception e) {
            //ignore
        }
    }
}
