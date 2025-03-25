package com.timgroup.statsd;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.SocketAddress;
import java.net.StandardProtocolFamily;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import jnr.unixsocket.UnixSocketAddress;
import jnr.unixsocket.UnixSocketChannel;
import jnr.unixsocket.UnixSocketOptions;

/** A ClientChannel for Unix domain sockets. */
public class UnixStreamClientChannel implements ClientChannel {
    private final SocketAddress address;
    private final int timeout;
    private final int connectionTimeout;
    private final int bufferSize;

    private SocketChannel delegate;
    private final ByteBuffer delimiterBuffer =
            ByteBuffer.allocateDirect(Integer.SIZE / Byte.SIZE).order(ByteOrder.LITTLE_ENDIAN);

    /**
     * Creates a new NamedPipeClientChannel with the given address.
     *
     * @param address Location of named pipe
     */
    UnixStreamClientChannel(
            SocketAddress address, int timeout, int connectionTimeout, int bufferSize)
            throws IOException {
        this.delegate = null;
        this.address = address;
        this.timeout = timeout;
        this.connectionTimeout = connectionTimeout;
        this.bufferSize = bufferSize;
    }

    @Override
    public boolean isOpen() {
        return delegate.isConnected();
    }

    @Override
    public synchronized int write(ByteBuffer src) throws IOException {
        connectIfNeeded();

        int size = src.remaining();
        int written = 0;
        if (size == 0) {
            return 0;
        }
        delimiterBuffer.clear();
        delimiterBuffer.putInt(size);
        delimiterBuffer.flip();

        try {
            long deadline = System.nanoTime() + timeout * 1_000_000L;
            written = writeAll(delimiterBuffer, true, deadline);
            if (written > 0) {
                written += writeAll(src, false, deadline);
            }
        } catch (IOException e) {
            // If we get an exception, it's unrecoverable, we close the channel and try to reconnect
            disconnect();
            throw e;
        }

        // If we haven't written anything, we have a timeout
        if (written == 0) {
            throw new IOException("Write timed out");
        }

        return size;
    }

    /**
     * Writes all bytes from the given buffer to the channel.
     *
     * @param bb buffer to write
     * @param canReturnOnTimeout if true, we return if the channel is blocking and we haven't
     *     written anything yet
     * @param deadline deadline for the write
     * @return number of bytes written
     * @throws IOException if the channel is closed or an error occurs
     */
    public int writeAll(ByteBuffer bb, boolean canReturnOnTimeout, long deadline)
            throws IOException {
        int remaining = bb.remaining();
        int written = 0;
        while (remaining > 0) {
            int read = delegate.write(bb);

            // If we haven't written anything yet, we can still return
            if (read == 0 && canReturnOnTimeout && written == 0) {
                return written;
            }

            remaining -= read;
            written += read;

            if (deadline > 0 && System.nanoTime() > deadline) {
                throw new IOException("Write timed out");
            }
        }
        return written;
    }

    private void connectIfNeeded() throws IOException {
        if (delegate == null) {
            connect();
        }
    }

    private void disconnect() throws IOException {
        if (delegate != null) {
            delegate.close();
            delegate = null;
        }
    }

    private void connect() throws IOException {
        if (this.delegate != null) {
            try {
                disconnect();
            } catch (IOException e) {
                // ignore to be sure we don't stay with a broken delegate forever.
            }
        }

        long deadline = System.nanoTime() + connectionTimeout * 1_000_000L;
        // Use native UDS support for compatible Java versions and jnr-unixsocket support otherwise.
        if (VersionUtils.isJavaVersionAtLeast(16)) {
            try {
                // Use reflection to avoid compiling Java 16+ classes in incompatible versions
                Class<?> protocolFamilyClass = Class.forName("java.net.StandardProtocolFamily");
                Object unixProtocol = Enum.valueOf((Class<Enum>) protocolFamilyClass, "UNIX");
                // Explicitly set StandardProtocolFamily.UNIX so that the socket uses the UDS protocol
                Method openMethod = SocketChannel.class.getMethod("open", protocolFamilyClass);
                // Open the socketchannel with the UDS protocol
                SocketChannel channel = (SocketChannel) openMethod.invoke(null, unixProtocol);
                
                if (connectionTimeout > 0) {
                    channel.socket().setSoTimeout(connectionTimeout);
                }
                try {
                    // socketchannel is failing to connect here :(
                    if (!channel.connect(address)) {
                        if (connectionTimeout > 0 && System.nanoTime() > deadline) {
                            throw new IOException("Connection timed out");
                        }
                        if (!channel.finishConnect()) {
                            throw new IOException("Connection failed");
                        }
                    }
                    channel.socket().setSoTimeout(Math.max(timeout, 0));
                    if (bufferSize > 0) {
                        channel.socket().setSendBufferSize(bufferSize);
                    }
                } catch (Exception e) {
                    try {
                        channel.close();
                    } catch (IOException __) {
                         // ignore
                    }
                    throw e;
                }
                
                this.delegate = channel;
                return;
            } catch (Exception e) {
                throw new IOException("Failed to create UnixStreamClientChannel for native UDS implementation", e);
            }
        }
        // Default to jnr-unixsocket if Java version is less than 16
        UnixSocketChannel channel = UnixSocketChannel.create();
        
        if (connectionTimeout > 0) {
            // Set connect timeout, this should work at least on linux
            // https://elixir.bootlin.com/linux/v5.7.4/source/net/unix/af_unix.c#L1696
            channel.setOption(UnixSocketOptions.SO_SNDTIMEO, connectionTimeout);
        }

        try {
            // Ensure address is of type UnixSocketAddress -- this should be unnecessary after native UDS support
            // is fixed and addresses that are not of type UnixSocketAddress are filtered out
            UnixSocketAddress unixAddress;
            if (address instanceof UnixSocketAddress) {
                unixAddress = (UnixSocketAddress) address;
            } else {
                unixAddress = new UnixSocketAddress(address.toString());
            }
            
            if (!channel.connect(unixAddress)) {
                if (connectionTimeout > 0 && System.nanoTime() > deadline) {
                    throw new IOException("Connection timed out");
                }
                if (!channel.finishConnect()) {
                    throw new IOException("Connection failed");
                }
            }

            channel.setOption(UnixSocketOptions.SO_SNDTIMEO, Math.max(timeout, 0));
            if (bufferSize > 0) {
                channel.setOption(UnixSocketOptions.SO_SNDBUF, bufferSize);
            }
        } catch (Exception e) {
            try {
                channel.close();
            } catch (IOException __) {
                // ignore
            }
            throw e;
        }

        this.delegate = channel;
    }

    @Override
    public void close() throws IOException {
        disconnect();
    }

    @Override
    public String getTransportType() {
        return "uds-stream";
    }

    @Override
    public String toString() {
        return "[" + getTransportType() + "] " + address;
    }

    @Override
    public int getMaxPacketSizeBytes() {
        return NonBlockingStatsDClient.DEFAULT_UDS_MAX_PACKET_SIZE_BYTES;
    }
}
