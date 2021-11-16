package com.timgroup.statsd;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;

public class NamedPipeClientChannel implements ClientChannel {
    private final RandomAccessFile randomAccessFile;
    private final FileChannel fileChannel;

    public NamedPipeClientChannel(NamedPipeSocketAddress address) throws FileNotFoundException {
        randomAccessFile = new RandomAccessFile(address.getPipe(), "rw");
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
}
