package com.timgroup.statsd;

import jnr.unixsocket.UnixDatagramChannel;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;

public class DatagramClientChannel implements ClientChannel {
    private final DatagramChannel delegate;
    private final String transport;
    private final SocketAddress address;

    /**
     * Creates a new DatagramClientChannel that wraps the delegate.
     * @param address Address to connect the channel to
     */
    public DatagramClientChannel(DatagramChannel delegate, SocketAddress address) throws IOException {
        this.delegate = delegate;

        if (delegate instanceof UnixDatagramChannel) {
            transport = "uds";
        } else {
            transport = "udp";
        }
        this.address = address;
    }

    @Override
    public boolean isOpen() {
        return delegate.isOpen();
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        return delegate.send(src, address);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public String getTransportType() {
        return transport;
    }
}
