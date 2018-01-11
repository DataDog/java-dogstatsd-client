package com.timgroup.statsd;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * Interface for abstracting communication-layer of the {@link StatsDClient}.
 *
 * @author Pascal Gélinas
 */
public interface Protocol extends Closeable, Flushable {

    /**
     * The {@link Charset} to use for transforming strings to bytes
     */
    Charset MESSAGE_CHARSET = StandardCharsets.UTF_8;
    /**
     * The default packet size.
     */
    int PACKET_SIZE_BYTES = 1400;

    /**
     * Send the specified StatsD-formatted message to the server. <p>Implementations are not
     * required to send the message right away and are free to do any buffering. </p>
     */
    void send(String message) throws IOException;
}
