package com.timgroup.statsd;

import java.nio.channels.WritableByteChannel;

interface ClientChannel extends WritableByteChannel {
    String getTransportType();

    int getMaxPacketSizeBytes();
}
