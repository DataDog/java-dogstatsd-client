package com.timgroup.statsd;

import java.net.InetSocketAddress;
import java.util.concurrent.Callable;

/**
 * A Blocking implementation of a StatsDClient. The metrics IO Operation is done in the calling
 * thread, and each metric is sent individually.
 */
public final class BlockingStatsDClient extends DefaultStatsDClient {

    private final Sender sender;

    /**
     * Create a new StatsD client communicating with a StatsD instance on the specified host and
     * port. All messages send via this client will have their keys prefixed with the specified
     * string. The new client will attempt to open a connection to the StatsD server immediately
     * upon instantiation, and may throw an exception if that a connection cannot be established.
     * Once a client has been instantiated in this way, all exceptions thrown during subsequent
     * usage are consumed, guaranteeing that failures in metrics will not affect normal code
     * execution.
     *
     * @param prefix the prefix to apply to keys sent via this client
     * @param hostname the host name of the targeted StatsD server
     * @param port the port of the targeted StatsD server
     * @throws StatsDClientException if the client could not be started
     */
    public BlockingStatsDClient(String prefix, String hostname, int port) {
        this(prefix, hostname, port, null);
    }

    /**
     * Create a new StatsD client communicating with a StatsD instance on the specified host and
     * port. All messages send via this client will have their keys prefixed with the specified
     * string. The new client will attempt to open a connection to the StatsD server immediately
     * upon instantiation, and may throw an exception if that a connection cannot be established.
     * Once a client has been instantiated in this way, all exceptions thrown during subsequent
     * usage are consumed, guaranteeing that failures in metrics will not affect normal code
     * execution.
     *
     * @param prefix the prefix to apply to keys sent via this client
     * @param hostname the host name of the targeted StatsD server
     * @param port the port of the targeted StatsD server
     * @param constantTags tags to be added to all content sent
     * @throws StatsDClientException if the client could not be started
     */
    public BlockingStatsDClient(String prefix, String hostname, int port, String[] constantTags) {
        this(prefix, hostname, port, constantTags, null);
    }

    /**
     * Create a new StatsD client communicating with a StatsD instance on the specified host and
     * port. All messages send via this client will have their keys prefixed with the specified
     * string. The new client will attempt to open a connection to the StatsD server immediately
     * upon instantiation, and may throw an exception if that a connection cannot be established.
     * Once a client has been instantiated in this way, all exceptions thrown during subsequent
     * usage are passed to the specified handler and then consumed, guaranteeing that failures in
     * metrics will not affect normal code execution.
     *
     * @param prefix the prefix to apply to keys sent via this client
     * @param hostname the host name of the targeted StatsD server
     * @param port the port of the targeted StatsD server
     * @param constantTags tags to be added to all content sent
     * @param errorHandler handler to use when an exception occurs during usage, may be null to
     * indicate noop
     * @throws StatsDClientException if the client could not be started
     */
    public BlockingStatsDClient(final String prefix, final String hostname, final int port,
        final String[] constantTags, final StatsDClientErrorHandler errorHandler)
        throws StatsDClientException {
        this(prefix, constantTags, errorHandler, staticStatsDAddressResolution(hostname, port));
    }

    /**
     * Create a new StatsD client communicating with a StatsD instance on the specified host and
     * port. All messages send via this client will have their keys prefixed with the specified
     * string. The new client will attempt to open a connection to the StatsD server immediately
     * upon instantiation, and may throw an exception if that a connection cannot be established.
     * Once a client has been instantiated in this way, all exceptions thrown during subsequent
     * usage are passed to the specified handler and then consumed, guaranteeing that failures in
     * metrics will not affect normal code execution.
     *
     * @param prefix the prefix to apply to keys sent via this client
     * @param constantTags tags to be added to all content sent
     * @param errorHandler handler to use when an exception occurs during usage, may be null to
     * indicate noop
     * @param addressLookup yields the IP address and socket of the StatsD server
     * @throws StatsDClientException if the client could not be started
     */
    public BlockingStatsDClient(String prefix, String[] constantTags,
        StatsDClientErrorHandler errorHandler, Callable<InetSocketAddress> addressLookup) {
        super(prefix, constantTags, errorHandler);
        sender = new Sender(addressLookup);
    }

    @Override
    protected void send(String message) {
        try {
            synchronized (sender) {
                sender.addToBuffer(message);
                sender.blockingSend();
            }
        } catch (Exception e) {
            handler.handle(e);
        }
    }

}
