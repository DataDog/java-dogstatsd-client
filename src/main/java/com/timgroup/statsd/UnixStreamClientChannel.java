package com.timgroup.statsd;

import java.io.IOException;
import java.net.SocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
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
    private final boolean enableJdkSocket;

    private SocketChannel delegate;
    private final ByteBuffer delimiterBuffer =
            ByteBuffer.allocateDirect(Integer.SIZE / Byte.SIZE).order(ByteOrder.LITTLE_ENDIAN);

    /**
     * Creates a new NamedPipeClientChannel with the given address.
     *
     * @param address Location of named pipe
     */
    UnixStreamClientChannel(
            SocketAddress address,
            int timeout,
            int connectionTimeout,
            int bufferSize,
            boolean enableJdkSocket)
            throws IOException {
        this.delegate = null;
        this.address = address;
        this.timeout = timeout;
        this.connectionTimeout = connectionTimeout;
        this.bufferSize = bufferSize;
        this.enableJdkSocket = enableJdkSocket;
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
            long deadline = timeout > 0 ? System.nanoTime() + timeout * 1_000_000L : 0;
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
        long timeoutMs = timeout;

        while (remaining > 0) {
            int read = delegate.write(bb);
            if (read > 0) {
                remaining -= read;
                written += read;
                continue;
            }

            if (read == 0) {
                if (delegate.isBlocking()) {
                    if (canReturnOnTimeout && written == 0) {
                        return written;
                    }
                    throw new IOException("Write timed out");
                }

                try (Selector selector = Selector.open()) {
                    delegate.register(selector, SelectionKey.OP_WRITE);
                    long selectTimeout = timeoutMs;

                    if (deadline > 0) {
                        long remainingNs = deadline - System.nanoTime();
                        if (remainingNs <= 0) {
                            throw new IOException("Write timed out");
                        }
                        long remainingMs = Math.max(1L, remainingNs / 1_000_000L);
                        selectTimeout =
                                timeoutMs > 0 ? Math.min(timeoutMs, remainingMs) : remainingMs;
                    }

                    int selected =
                            selectTimeout > 0 ? selector.select(selectTimeout) : selector.select();
                    if (selected == 0) {
                        throw new IOException("Write timed out after " + selectTimeout + "ms");
                    }
                }
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

        // Use native JDK support for UDS on Java 16+ and jnr-unixsocket otherwise
        if (VersionUtils.isJavaVersionAtLeast(16) && enableJdkSocket && connectWithJdkSocket()) {
            return;
        }
        // Default to jnr-unixsocket if Java version is < 16 or native support is disabled
        UnixSocketChannel channel = UnixSocketChannel.create();
        long deadline = System.nanoTime() + connectionTimeout * 1_000_000L;

        if (connectionTimeout > 0) {
            // Set connect timeout, this should work at least on linux
            // https://elixir.bootlin.com/linux/v5.7.4/source/net/unix/af_unix.c#L1696
            channel.setOption(UnixSocketOptions.SO_SNDTIMEO, connectionTimeout);
        }

        try {
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

    private boolean connectWithJdkSocket() throws IOException {
        SocketChannel channel = null;
        SocketAddress connectAddress;

        try {
            // Only SocketChannel.open(ProtocolFamily) needs reflection; connect/finishConnect
            // have existed since Java 1.4 and are called directly once we have the channel.
            channel = openJdkSocketChannel();
            connectAddress = nativeSocketAddress(address);
            if (bufferSize > 0) {
                channel.setOption(StandardSocketOptions.SO_SNDBUF, bufferSize);
            }
        } catch (Exception | LinkageError e) {
            closeQuietly(channel);
            return false;
        }

        try {
            if (connectionTimeout <= 0) {
                channel.configureBlocking(true);
                channel.connect(connectAddress);
                channel.configureBlocking(false);
            } else {
                channel.configureBlocking(false);
                long deadline = System.nanoTime() + connectionTimeout * 1_000_000L;
                if (!channel.connect(connectAddress)) {
                    try (Selector selector = Selector.open()) {
                        channel.register(selector, SelectionKey.OP_CONNECT);
                        while (!channel.finishConnect()) {
                            long remainingNs = deadline - System.nanoTime();
                            if (remainingNs <= 0) {
                                throw new IOException(
                                        "Connection timed out after " + connectionTimeout + "ms");
                            }
                            long selectTimeout = Math.max(1L, remainingNs / 1_000_000L);
                            if (selector.select(selectTimeout) == 0
                                    && System.nanoTime() >= deadline) {
                                throw new IOException(
                                        "Connection timed out after " + connectionTimeout + "ms");
                            }
                            selector.selectedKeys().clear();
                        }
                    }
                }
            }
        } catch (IOException | RuntimeException e) {
            closeQuietly(channel);
            throw e;
        }

        this.delegate = channel;
        return true;
    }

    SocketChannel openJdkSocketChannel() throws IOException {
        return VersionUtils.openUnixSocketChannel();
    }

    private static SocketAddress nativeSocketAddress(SocketAddress address) {
        if (address instanceof UnixSocketAddressWithTransport) {
            address = ((UnixSocketAddressWithTransport) address).getAddress();
        }
        if (address instanceof UnixSocketAddress) {
            return VersionUtils.newUnixDomainSocketAddress(((UnixSocketAddress) address).path());
        }
        return address;
    }

    private static void closeQuietly(SocketChannel channel) {
        if (channel != null) {
            try {
                channel.close();
            } catch (IOException ignored) {
                // ignore
            }
        }
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
