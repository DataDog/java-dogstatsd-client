package com.timgroup.statsd;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import jnr.unixsocket.UnixSocketAddress;
import jnr.unixsocket.UnixSocketChannel;
import jnr.unixsocket.UnixSocketOptions;

public class UnixStreamClientChannel implements ClientChannel {
    private final UnixSocketAddress address;
    private final int timeout;
    private final int bufferSize;

    private SocketChannel delegate;
    private final ByteBuffer delimiterBuffer = ByteBuffer.allocateDirect(Integer.SIZE / Byte.SIZE).order(ByteOrder.LITTLE_ENDIAN);

    /**
     * Creates a new NamedPipeClientChannel with the given address.
     *
     * @param address Location of named pipe
     */
    UnixStreamClientChannel(SocketAddress address, int timeout, int bufferSize) throws IOException {
        this.delegate = null;
        this.address = (UnixSocketAddress) address;
        this.timeout = timeout;
        this.bufferSize = bufferSize;
    }

    @Override
    public boolean isOpen() {
        return delegate.isConnected();
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        if (delegate == null || !delegate.isConnected()) {
            connect();
        }

        int size = src.remaining();
        if (size == 0) {
            return 0;
        }
        delimiterBuffer.clear();
        delimiterBuffer.putInt(size);
        delimiterBuffer.flip();

        try {
            if (writeAll(delimiterBuffer, true) > 0) {
                writeAll(src, false);
            }
        } catch (IOException e) {
            delegate.close();
            delegate = null;
            throw e;
        }

        return size;
    }

    public int writeAll(ByteBuffer bb, boolean canReturnOnTimeout) throws IOException {
        int remaining = bb.remaining();
        int written = 0;
        while (remaining > 0) {
            int n = delegate.write(bb);

            // If we haven't written anything yet, we can still return
            if (n == 0 && canReturnOnTimeout && written == 0) {
                return written;
            }

            remaining -= n;
            written += n;
        }
        return written;
    }

    private void connect() throws IOException {
        if (this.delegate != null) {
            try {
                this.delegate.close();
                this.delegate = null;
            } catch (IOException e) {
                // ignore to be sure we don't stay with a broken delegate forever.
            }
        }
        this.delegate = UnixSocketChannel.open(address);
        if (timeout > 0) {
            delegate.setOption(UnixSocketOptions.SO_SNDTIMEO, timeout);
        }
        if (bufferSize > 0) {
            delegate.setOption(UnixSocketOptions.SO_SNDBUF, bufferSize);
        }
    }

    @Override
    public void close() throws IOException {
        // closing the file also closes the channel
        if (delegate != null) {
            delegate.close();
        }
    }

    @Override
    public String getTransportType() {
        return "uds-stream";
    }

    @Override
    public String toString() {
        return "[" + getTransportType() + "] " + address;
    }
}
