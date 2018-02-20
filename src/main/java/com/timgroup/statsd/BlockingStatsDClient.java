package com.timgroup.statsd;

import java.io.Flushable;
import java.io.IOException;

/**
 * A Blocking implementation of a StatsDClient. The metrics IO Operation is done in the calling
 * thread, and each metric can be sent individually when configured with the autoFlush flag.
 * <p>Thread-safety note: this class is thread-safe if and only if the underlying protocol is
 * thread-safe. The default {@link UdpProtocol} is NOT thread-safe.</p>
 *
 * @see NonBlockingStatsDClient
 *
 * @author Pascal Gélinas
 */
public final class BlockingStatsDClient extends DefaultStatsDClient implements Flushable {

    private final boolean autoflush;
    private final Protocol protocol;

    /**
     * Create a new StatsD client communicating with a StatsD instance on the specified host and
     * port. All messages send via this client will have their keys prefixed with the specified
     * string. The new client will attempt to open a connection to the StatsD server immediately
     * upon instantiation, and may throw an exception if that a connection cannot be established.
     * Once a client has been instantiated in this way, all exceptions thrown during subsequent
     * usage are consumed, guaranteeing that failures in metrics will not affect normal code
     * execution. <p>The client will use the UDP protcol to communicate with the StatsD
     * instance.</p>
     *
     * @param prefix
     *     the prefix to apply to keys sent via this client
     * @param hostname
     *     the host name of the targeted StatsD server
     * @param port
     *     the port of the targeted StatsD server
     * @throws StatsDClientException
     *     if the client could not be started
     */
    public BlockingStatsDClient(String prefix, String hostname, int port) {
        this(prefix, hostname, port, true);
    }

    /**
     * Create a new StatsD client communicating with a StatsD instance on the specified host and
     * port. All messages send via this client will have their keys prefixed with the specified
     * string. The new client will attempt to open a connection to the StatsD server immediately
     * upon instantiation, and may throw an exception if that a connection cannot be established.
     * Once a client has been instantiated in this way, all exceptions thrown during subsequent
     * usage are consumed, guaranteeing that failures in metrics will not affect normal code
     * execution. <p>The client will use the UDP protcol to communicate with the StatsD
     * instance.</p>
     *
     * @param prefix
     *     the prefix to apply to keys sent via this client
     * @param hostname
     *     the host name of the targeted StatsD server
     * @param port
     *     the port of the targeted StatsD server
     * @param autoflush
     *     controls whether the IO Buffer is flushed after each metric call.
     * @throws StatsDClientException
     *     if the client could not be started
     */
    public BlockingStatsDClient(String prefix, String hostname, int port, boolean autoflush) {
        this(prefix, autoflush, null, null, createStatsDProtocol(hostname, port));
    }

    /**
     * Create a new StatsD client communicating with a StatsD instance using the specified protocol.
     * All messages send via this client will have their keys prefixed with the specified string.
     * Once a client has been instantiated in this way, all exceptions thrown during subsequent
     * usage are passed to the specified handler and then consumed, guaranteeing that failures in
     * metrics will not affect normal code execution. <p>Prefer using the {@link
     * StatsDClientBuilder} over this constructor.</p>
     *
     * @param prefix
     *     the prefix to apply to keys sent via this client
     * @param autoflush
     *     controls whether the IO Buffer is flushed after each metric call.
     * @param constantTags
     *     tags to be added to all content sent
     * @param errorHandler
     *     handler to use when an exception occurs during usage, may be null to indicate noop
     * @param protocol
     *     the underlying protocol to use for communication.
     * @throws StatsDClientException
     *     if the client could not be started
     */
    public BlockingStatsDClient(String prefix, boolean autoflush, String[] constantTags,
        StatsDClientErrorHandler errorHandler, Protocol protocol) {
        super(prefix, constantTags, errorHandler);
        this.autoflush = autoflush;
        this.protocol = protocol;
    }

    @Override
    protected void send(String message) {
        try {
            protocol.send(message);
            if (autoflush) {
                protocol.flush();
            }
        } catch (IOException e) {
            handler.handle(e);
        }
    }

    @Override
    public void stop() {
        try (Protocol protocol = this.protocol) {
            protocol.flush();
        } catch (IOException e) {
            handler.handle(e);
        }
    }

    @Override
    public void flush() throws IOException {
        protocol.flush();
    }
}
