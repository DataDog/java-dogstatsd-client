package com.timgroup.statsd;

import java.net.SocketAddress;

public class NamedPipeSocketAddress extends SocketAddress {
    private static final String NAMED_PIPE_PREFIX = "\\\\.\\pipe\\";
    private final String pipe;

    public NamedPipeSocketAddress(String pipeName) {
        this.pipe = pipeName.startsWith(NAMED_PIPE_PREFIX) ? pipeName : NAMED_PIPE_PREFIX + pipeName;;
    }

    public String getPipe() {
        return pipe;
    }
}
