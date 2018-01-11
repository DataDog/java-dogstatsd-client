package com.timgroup.statsd;

import java.io.IOException;

/**
 * Builder for the different {@link StatsDClient} imlpementations.
 *
 * @author Pascal Gélinas
 */
public final class StatsDClientBuilder {

    private String prefix;
    private String hostname;
    private int port;
    private String[] constantTags;
    private StatsDClientErrorHandler errorHandler;

    private boolean useUdp;
    private Protocol customProtocol;
    // TODO
//    private boolean useTcp;

    /**
     * @param prefix the prefix to apply to keys sent via this client
     * @return this
     */
    public StatsDClientBuilder prefix(String prefix) {
        this.prefix = prefix;
        return this;
    }

    /**
     * @param hostname the host name of the targeted StatsD server
     * @return this
     */
    public StatsDClientBuilder hostname(String hostname) {
        this.hostname = hostname;
        return this;
    }

    /**
     * @param port the port of the targeted StatsD server
     * @return this
     */
    public StatsDClientBuilder port(int port) {
        this.port = port;
        return this;
    }

    /**
     * @param constantTags tags to be added to all content sent
     * @return this
     */
    public StatsDClientBuilder constantTags(String... constantTags) {
        this.constantTags = constantTags;
        return this;
    }

    /**
     * @param errorHandler handler to use when an exception occurs during usage, may be null to
     * indicate noop
     *
     * @return this
     */
    public StatsDClientBuilder errorHandler(
        StatsDClientErrorHandler errorHandler) {
        this.errorHandler = errorHandler;
        return this;
    }

    /**
     * Indicate that the clients built by this builder use the UDP protocol.
     *
     * @return this
     */
    public StatsDClientBuilder udpProtocol() {
        this.useUdp = true;
        return this;
    }

    /**
     * @param protocol A custom {@link Protocol} implementation to use by the client.
     *
     * @return this
     */
    public StatsDClientBuilder customProtocol(Protocol protocol) {
        this.customProtocol = protocol;
        return this;
    }

    private Protocol createProtocol() {
        if (customProtocol != null) {
            return customProtocol;
        }
        // FIXME always use Udp since there are no alternative for now.
        try {
            return new UdpProtocol(
                DefaultStatsDClient.staticStatsDAddressResolution(hostname, port), errorHandler);
        } catch (IOException e) {
            throw new StatsDClientException("Unable to create protcol.", e);
        }
    }

    /**
     * Build a StatsDClient client in non-blocking mode with the specified parameters of this
     * builder.
     * @return A {@link NonBlockingStatsDClient}
     */
    public StatsDClient buildNonBlocking() {
        return buildNonBlocking(Integer.MAX_VALUE);
    }

    /**
     * Build a StatsDClient client in non-blocking mode with the specified parameters of this
     * builder.
     * @param queueSize the maximum amount of unprocessed messages in the BlockingQueue.
     * @return A {@link NonBlockingStatsDClient} with the specified queue size.
     */
    public StatsDClient buildNonBlocking(int queueSize) {
        return new NonBlockingStatsDClient(prefix, queueSize, constantTags, errorHandler,
            createProtocol());
    }

    /**
     * Build a StatsDClient client in concurrent mode with the specified parameters of this builder.
     * @return A {@link ConcurrentStatsDClient}
     */
    public StatsDClient buildConcurrent() {
        return buildConcurrent(1000);
    }

    /**
     * Build a StatsDClient client in concurrent mode with the specified parameters of this builder.
     * @param waitResolution the time, in millis, the background IO thread waits before polling
     * the metric queue.
     * @return A {@link ConcurrentStatsDClient} with the specified wait time.
     */
    public StatsDClient buildConcurrent(long waitResolution) {
        return new ConcurrentStatsDClient(prefix, waitResolution, constantTags, errorHandler,
            createProtocol());
    }

    /**
     * Build a StatsDClient client in blocking mode with the specified parameters of this builder.
     *
     * @return A {@link BlockingStatsDClient}
     */
    public StatsDClient buildBlocking() {
        return buildBlocking(true);
    }

    /**
     * Build a StatsDClient client in blocking mode with the specified parameters of this builder.
     *
     * @param autoflush controls whether the underlying {@link Protocol} is flushed after each
     * metric call.
     * @return A {@link BlockingStatsDClient} with the specified autoflush behavior.
     */
    public StatsDClient buildBlocking(boolean autoflush) {
        return new BlockingStatsDClient(prefix, autoflush, constantTags, errorHandler,
            createProtocol());
    }
}
