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
     * Return true if object is a NamedPipeSocketAddress referring to the same path.
     */
    public boolean equals(Object object) {
        if (object instanceof NamedPipeSocketAddress) {
            return pipe.equals(((NamedPipeSocketAddress)object).pipe);
        }
        return false;
    }

    /**
     * A normalized version of the pipe name that includes the `\\.\pipe\` prefix
     */
    static String normalizePipeName(String pipeName) {
        if (pipeName.startsWith(NAMED_PIPE_PREFIX)) {
            return pipeName;
        } else {
            return NAMED_PIPE_PREFIX + pipeName;
        }
    }

    static boolean isNamedPipe(String address) {
        return address.startsWith(NAMED_PIPE_PREFIX);
    }
}
