package com.timgroup.statsd;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;

/**
 * A ClientChannel for Windows named pipes.
 *
 * <p>The pipe is opened on the first write, not when the client is built, so the client can start
 * before the server creates the pipe. After a failed write the pipe is closed and the next write
 * opens it again, so the client recovers when the server restarts. Once {@link #close()} is called,
 * the channel stays closed and writes throw {@link ClosedChannelException}.
 */
class NamedPipeClientChannel implements ClientChannel {
    private final String pipe;
    private RandomAccessFile randomAccessFile;
    private FileChannel fileChannel;
    private boolean closed;

    /**
     * Creates a new NamedPipeClientChannel with the given address.
     *
     * @param address Location of named pipe
     */
    NamedPipeClientChannel(NamedPipeSocketAddress address) {
        pipe = address.getPipe();
    }

    @Override
    public synchronized boolean isOpen() {
        return !closed;
    }

    @Override
    public synchronized int write(ByteBuffer src) throws IOException {
        if (closed) {
            throw new ClosedChannelException();
        }
        connectIfNeeded();
        try {
            return fileChannel.write(src);
        } catch (IOException e) {
            // The pipe is broken, for example because the server restarted. Reopen on next write.
            try {
                disconnect();
            } catch (IOException ignored) {
                // Report the write error, not the close error.
            }
            throw e;
        }
    }

    private void connectIfNeeded() throws IOException {
        if (fileChannel == null) {
            randomAccessFile = new RandomAccessFile(pipe, "rw");
            fileChannel = randomAccessFile.getChannel();
        }
    }

    private void disconnect() throws IOException {
        if (randomAccessFile != null) {
            try {
                // closing the file also closes the channel
                randomAccessFile.close();
            } finally {
                randomAccessFile = null;
                fileChannel = null;
            }
        }
    }

    @Override
    public synchronized void close() throws IOException {
        closed = true;
        disconnect();
    }

    @Override
    public String getTransportType() {
        return "namedpipe";
    }

    @Override
    public String toString() {
        return pipe;
    }

    @Override
    public int getMaxPacketSizeBytes() {
        return NonBlockingStatsDClient.DEFAULT_UDS_MAX_PACKET_SIZE_BYTES;
    }
}
