package com.timgroup.statsd;

import jnr.unixsocket.UnixSocketAddress;
import jnr.unixsocket.UnixSocketChannel;
import jnr.unixsocket.UnixSocketOptions;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;

/**
 * A ClientChannel for Unix domain sockets.
 */
public class UnixStreamClientChannel implements ClientChannel {
    private final SocketAddress address;
    private final int timeout;
    private final int connectionTimeout;
    private final int bufferSize;

    private SocketChannel delegate;
    private final ByteBuffer delimiterBuffer = ByteBuffer.allocateDirect(Integer.SIZE / Byte.SIZE).order(ByteOrder.LITTLE_ENDIAN);

    /**
     * Creates a new NamedPipeClientChannel with the given address.
     *
     * @param address Location of named pipe
     */
    UnixStreamClientChannel(SocketAddress address, int timeout, int connectionTimeout, int bufferSize) throws IOException {
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
        // Use native JDK Unix domain socket support for compatible versions (Java 16+). Fall back to JNR support otherwise.
        if (ClientChannelUtils.hasNativeUdsSupport()) {
            connectJdkSocket(deadline);
        } else {
            connectJnrSocket(deadline);
        }
    }

    private void connectJdkSocket(long deadline) throws IOException {
        String socketPath;
        if (address instanceof UnixSocketAddress) {
            UnixSocketAddress unixAddr = (UnixSocketAddress) address;
            socketPath = unixAddr.path();
        } else {
            socketPath = address.toString();
            if (socketPath.startsWith("file://") || socketPath.startsWith("unix://")) {
                socketPath = socketPath.substring(7);
            }
        }
        
        try {
            // Use reflection to avoid compile-time dependency on Java 16+ classes
            Class<?> udsAddressClass = Class.forName("java.net.UnixDomainSocketAddress");
            Object udsAddress = udsAddressClass.getMethod("of", String.class).invoke(null, socketPath);
            
            SocketChannel delegate = SocketChannel.open();
            if (connectionTimeout > 0) {
                delegate.socket().setSoTimeout(connectionTimeout);
            }
            
            try {
                delegate.configureBlocking(false);
                if (!delegate.connect((SocketAddress) udsAddress)) {
                    if (connectionTimeout > 0 && System.nanoTime() > deadline) {
                        throw new IOException("Connection timed out");
                    }
                    if (!delegate.finishConnect()) {
                        throw new IOException("Connection failed");
                    }
                }
                delegate.configureBlocking(true);
                delegate.socket().setSoTimeout(Math.max(timeout, 0));
                if (bufferSize > 0) {
                    delegate.socket().setSendBufferSize(bufferSize);
                }
                this.delegate = delegate;
            } catch (Exception e) {
                try {
                    delegate.close();
                } catch (IOException __) {
                    // ignore
                }
                throw new IOException("Failed to connect to Unix Domain Socket: " + socketPath, e);
            }
        } catch (ReflectiveOperationException e) {
            throw new IOException("Failed to create UnixDomainSocketAddress: Java 16+ required", e);
        }
    }

    private void connectJnrSocket(long deadline) throws IOException {
        UnixSocketChannel delegate = UnixSocketChannel.create();
        if (connectionTimeout > 0) {
            // Set connect timeout, this should work at least on linux
            // https://elixir.bootlin.com/linux/v5.7.4/source/net/unix/af_unix.c#L1696
            delegate.setOption(UnixSocketOptions.SO_SNDTIMEO, connectionTimeout);
        }
        try {
            if (!delegate.connect((UnixSocketAddress) address)) {
                if (connectionTimeout > 0 && System.nanoTime() > deadline) {
                    throw new IOException("Connection timed out");
                }
                if (!delegate.finishConnect()) {
                    throw new IOException("Connection failed");
                }
            }
            delegate.setOption(UnixSocketOptions.SO_SNDTIMEO, Math.max(timeout, 0));
            if (bufferSize > 0) {
                delegate.setOption(UnixSocketOptions.SO_SNDBUF, bufferSize);
            }
            this.delegate = delegate;
        } catch (Exception e) {
            try {
                delegate.close();
            } catch (IOException __) {
                // ignore
            }
            throw e;
        }
    }

    /**
     * Writes all bytes from the given buffer to the channel.
     * @param bb buffer to write
     * @param canReturnOnTimeout if true, we return if the channel is blocking and we haven't written anything yet
     * @param deadline deadline for the write
     * @return number of bytes written
     * @throws IOException if the channel is closed or an error occurs
     */
    public int writeAll(ByteBuffer bb, boolean canReturnOnTimeout, long deadline) throws IOException {
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
