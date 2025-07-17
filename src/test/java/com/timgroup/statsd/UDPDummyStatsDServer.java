package com.timgroup.statsd;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;

public class UDPDummyStatsDServer extends DummyStatsDServer {
    private final DatagramChannel server;

    public UDPDummyStatsDServer(int port) throws IOException {
        server = DatagramChannel.open();
        server.bind(new InetSocketAddress(port));
        this.listen();
    }

    @Override
    protected boolean isOpen() {
        return server.isOpen();
    }

    @Override
    protected void receive(ByteBuffer packet) throws IOException {
        server.receive(packet);
    }

    public void close() throws IOException {
        try {
            server.close();
        } catch (Exception e) {
            // ignore
        }
    }

    int getPort() throws IOException {
        return ((InetSocketAddress) server.getLocalAddress()).getPort();
    }
}
