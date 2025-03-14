package com.timgroup.statsd;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import jnr.unixsocket.UnixDatagramChannel;
import jnr.unixsocket.UnixSocketAddress;

import static com.timgroup.statsd.NonBlockingStatsDClient.DEFAULT_UDS_MAX_PACKET_SIZE_BYTES;

public class UnixDatagramSocketDummyStatsDServer extends DummyStatsDServer {
    private final DatagramChannel server;
    private volatile boolean running = true;

    public UnixDatagramSocketDummyStatsDServer(String socketPath) throws IOException {
        server = UnixDatagramChannel.open();
        server.bind(new UnixSocketAddress(socketPath));
        this.listen();
    }

    @Override
    protected boolean isOpen() {
        return running && server.isOpen();
    }

    protected void receive(ByteBuffer packet) throws IOException {
        server.receive(packet);
    }

    @Override
    public void close() throws IOException {
        running = false;  // Signal the listening thread to stop
        try {
            // Give the listening thread a chance to stop
            Thread.sleep(50);
        } catch (InterruptedException e) {
            // Ignore
        }
        try {
            server.close();
        } catch (Exception e) {
            //ignore
        }
    }
}
