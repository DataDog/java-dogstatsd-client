package com.timgroup.statsd;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

public interface ClientChannel extends WritableByteChannel {
    String getTransportType();
}
