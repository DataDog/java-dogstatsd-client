package com.timgroup.statsd;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class NamedPipeClientChannel implements ClientChannel {
    private final RandomAccessFile randomAccessFile;
    private final FileChannel fileChannel;
    private final String pipe;

    /**
     * Creates a new NamedPipeClientChannel with the given address.
     */
    public NamedPipeClientChannel(NamedPipeSocketAddress address) throws FileNotFoundException {
        pipe = address.getPipe();
        randomAccessFile = new RandomAccessFile(pipe, "rw");
        fileChannel = randomAccessFile.getChannel();
    }

    @Override
    public boolean isOpen() {
        return fileChannel.isOpen();
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        return fileChannel.write(src);
    }

    @Override
    public void close() throws IOException {
        // closing the file also closes the channel
        randomAccessFile.close();
    }

    @Override
    public String getTransportType() {
        return "namedpipe";
    }

    @Override
    public String toString() {
        return pipe;
    }
}
