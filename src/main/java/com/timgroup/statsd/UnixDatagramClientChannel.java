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
        super(ClientChannelUtils.isJavaVersionAtLeast(16) ? DatagramChannel.open() : UnixDatagramChannel.open(), address);
        if (ClientChannelUtils.isJavaVersionAtLeast(16)) {
            if (timeout > 0) {
                delegate.socket().setSoTimeout(timeout);
            }
            if (bufferSize > 0) {
                delegate.socket().setSendBufferSize(bufferSize);
            }
        } else {
            // Set send timeout, to handle the case where the transmission buffer is full
            // If no timeout is set, the send becomes blocking
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
