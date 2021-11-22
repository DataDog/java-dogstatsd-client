package com.timgroup.statsd;

import java.net.SocketAddress;

public class NamedPipeSocketAddress extends SocketAddress {
    private static final String NAMED_PIPE_PREFIX = "\\\\.\\pipe\\";
    private final String pipe;

    public NamedPipeSocketAddress(String pipeName) {
        this.pipe = normalizePipeName(pipeName);
    }

    public String getPipe() {
        return pipe;
    }

    /**
     * A normalized version of the pipe name that includes the `\\.\pipe\` prefix
     */
    public static String normalizePipeName(String pipeName) {
        if (pipeName.startsWith(NAMED_PIPE_PREFIX)) {
            return pipeName;
        } else {
            return NAMED_PIPE_PREFIX + pipeName;
        }
    }
}
