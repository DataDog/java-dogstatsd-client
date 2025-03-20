package com.timgroup.statsd;

import jnr.unixsocket.UnixDatagramChannel;
import jnr.unixsocket.UnixSocketAddress;
import jnr.unixsocket.UnixSocketOptions;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
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
        super(createChannel(address, timeout, bufferSize), address);
    }

    private static DatagramChannel createChannel(SocketAddress address, int timeout, int bufferSize) throws IOException {
        // Use native UDS support for compatible Java versions and jnr-unixsocket support otherwise.
        if (VersionUtils.isJavaVersionAtLeast(16)) {
            try {
                // Use reflection to avoid compiling Java 16+ classes in incompatible versions
                Class<?> protocolFamilyClass = Class.forName("java.net.StandardProtocolFamily");
                Object unixProtocol = Enum.valueOf((Class<Enum>) protocolFamilyClass, "UNIX");
                Method openMethod = DatagramChannel.class.getMethod("open", protocolFamilyClass);
                DatagramChannel channel = (DatagramChannel) openMethod.invoke(null, unixProtocol);
                
                if (timeout > 0) {
                    channel.socket().setSoTimeout(timeout);
                }
                if (bufferSize > 0) {
                    channel.socket().setSendBufferSize(bufferSize);
                }
                return channel;
            } catch (Exception e) {
                throw new IOException("Failed to create UnixDatagramClientChannel for native UDS implementation", e);
            }
        }
        UnixDatagramChannel channel = UnixDatagramChannel.open();
        // Set send timeout, to handle the case where the transmission buffer is full
        // If no timeout is set, the send becomes blocking
        if (timeout > 0) {
            channel.setOption(UnixSocketOptions.SO_SNDTIMEO, timeout);
        }
        if (bufferSize > 0) {
            channel.setOption(UnixSocketOptions.SO_SNDBUF, bufferSize);
        }
        return channel;
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
