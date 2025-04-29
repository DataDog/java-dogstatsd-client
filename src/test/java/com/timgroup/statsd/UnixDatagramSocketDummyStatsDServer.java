package com.timgroup.statsd;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import jnr.unixsocket.UnixDatagramChannel;
import jnr.unixsocket.UnixSocketAddress;

import static com.timgroup.statsd.NonBlockingStatsDClient.DEFAULT_UDS_MAX_PACKET_SIZE_BYTES;

public class UnixDatagramSocketDummyStatsDServer extends DummyStatsDServer {
    private final DatagramChannel server;
    UnixSocketAddress addr;

    public UnixDatagramSocketDummyStatsDServer(String socketPath) throws IOException {
        server = UnixDatagramChannel.open();
        addr = new UnixSocketAddress(socketPath);
        server.bind(addr);
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
        if (!server.isOpen()) {
            return;
        }
        thread.interrupt();
        // JNR doesn't interrupt syscalls when a thread is interrupted, so we send a dummy message
        // to wake the thread up.
        int sent = server.send(ByteBuffer.wrap(new byte[]{1}), addr);
        if (sent == 0) {
            throw new IOException("failed to send wake up call to the server thread");
        }
        server.close();
        try {
            thread.join();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
}
