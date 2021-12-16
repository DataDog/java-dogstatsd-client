package com.timgroup.statsd;

import java.nio.channels.WritableByteChannel;

public interface ClientChannel extends WritableByteChannel {
    String getTransportType();
}
