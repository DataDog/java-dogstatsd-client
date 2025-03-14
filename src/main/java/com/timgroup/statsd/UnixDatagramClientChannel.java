package com.timgroup.statsd;

import jnr.unixsocket.UnixDatagramChannel;
import jnr.unixsocket.UnixSocketOptions;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.DatagramChannel;

class UnixDatagramClientChannel extends DatagramClientChannel {
    /**
     * Creates a new UnixDatagramClientChannel.
     *
     * @param address Address to connect the channel to
     * @param timeout Send timeout
     * @param bufferSize Buffer size
     * @throws IOException if socket options cannot be set
     */
    UnixDatagramClientChannel(SocketAddress address, int timeout, int bufferSize) throws IOException {
        super(createChannel(address), address);
        configureChannel(timeout, bufferSize);
    }

    private static DatagramChannel createChannel(SocketAddress address) throws IOException {
        if (ClientChannelUtils.hasNativeUdsSupport()) {
            return DatagramChannel.open();
        } else {
            return UnixDatagramChannel.open();
        }
    }

    private void configureChannel(int timeout, int bufferSize) throws IOException {
        if (ClientChannelUtils.hasNativeUdsSupport()) {
            if (timeout > 0) {
                delegate.socket().setSoTimeout(timeout);
            }
            if (bufferSize > 0) {
                delegate.socket().setSendBufferSize(bufferSize);
            }
        } else {
            if (timeout > 0) {
                delegate.setOption(UnixSocketOptions.SO_SNDTIMEO, timeout);
            }
            if (bufferSize > 0) {
                delegate.setOption(UnixSocketOptions.SO_SNDBUF, bufferSize);
            }
        }
    }

    @Override
    public String getTransportType() {
        return "uds";
    }

    @Override
    public int getMaxPacketSizeBytes() {
        return NonBlockingStatsDClient.DEFAULT_UDS_MAX_PACKET_SIZE_BYTES;
    }
}
