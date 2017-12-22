package com.timgroup.statsd;

/**
 * Builder for the different {@link StatsDClient} imlpementations.
 */
public final class StatsDClientBuilder {

    private String prefix;
    private String hostname;
    private int port;
    private String[] constantTags;
    private StatsDClientErrorHandler errorHandler;

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
    public StatsDClientBuilder constantTags(String[] constantTags) {
        this.constantTags = constantTags;
        return this;
    }

    /**
     * * @param errorHandler handler to use when an exception occurs during usage, may be null to
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
     * @return A {@link NonBlockingStatsDClient}
     */
    public StatsDClient buildNonBlocking() {
        return new NonBlockingStatsDClient(prefix, hostname, port, constantTags, errorHandler);
    }

    /**
     *
     * @param queueSize the maximum amount of unprocessed messages in the BlockingQueue.
     * @return A {@link NonBlockingStatsDClient} with the specified queue size.
     */
    public StatsDClient buildNonBlocking(int queueSize) {
        return new NonBlockingStatsDClient(prefix, hostname, port, queueSize, constantTags,
            errorHandler);
    }

    /**
     *
     * @return A {@link ConcurrentStatsDClient}
     */
    public StatsDClient buildConcurrent() {
        return new ConcurrentStatsDClient(prefix, hostname, port, constantTags, errorHandler);
    }

    /**
     *
     * @param waitResolution the time, in millis, the background IO thread waits before polling
     * the metric queue.
     * @return A {@link ConcurrentStatsDClient} with the specified wait time.
     */
    public StatsDClient buildConcurrent(long waitResolution) {
        return new ConcurrentStatsDClient(prefix, hostname, port, waitResolution, constantTags,
            errorHandler);
    }

    /**
     *
     * @return A {@link BlockingStatsDClient}
     */
    public StatsDClient buildBlocking() {
        return new BlockingStatsDClient(prefix, hostname, port, constantTags, errorHandler);
    }

    /**
     *
     * @param autoflush controls whether the IO Buffer is flushed after each metric call.
     * @return A {@link BlockingStatsDClient} with the specified autoflush behavior.
     */
    public StatsDClient buildBlocking(boolean autoflush) {
        return new BlockingStatsDClient(prefix, hostname, port, autoflush, constantTags,
            errorHandler);
    }
}
