package com.timgroup.statsd;

import java.net.InetSocketAddress;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * A StatsDClient implementation for usage in highly threaded situation. <p> Under high load, the
 * {@link java.util.concurrent.BlockingQueue} used by {@link NonBlockingStatsDClient} for
 * inter-thread communication can become a contention point, since multiple thread are trying to
 * obtain the Write Lock on the queue to insert metrics elements. With the {@link
 * ConcurrentStatsDClient}, it uses a non-blocking, lock-free queue for inter-thread communication
 * which remove that contention point. However, since this is a non-blocking queue, the background
 * thread needs to wait for element to come into the queue using a separate mechanism; currently it
 * uses {@link Thread#sleep(long)} to achieve this, which may hurt metric reporting and
 * responsiveness. </p> <p> {@link NonBlockingStatsDClient} will perform better than {@link
 * ConcurrentStatsDClient} under low to moderate load, but {@link ConcurrentStatsDClient}
 * outperforms {@link NonBlockingStatsDClient} under moderate to high load. Proper benchmarking by
 * the client application needs to be done to decide which implementation to choose from.</p>
 */
public final class ConcurrentStatsDClient extends BackgroundStatsDClient {

    private final ConcurrentLinkedQueue<String> queue;
    private final int waitResolution;

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
    public ConcurrentStatsDClient(String prefix, String hostname, int port) {
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
    public ConcurrentStatsDClient(String prefix, String hostname, int port, String[] constantTags) {
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
    public ConcurrentStatsDClient(final String prefix, final String hostname, final int port,
        final String[] constantTags, final StatsDClientErrorHandler errorHandler)
        throws StatsDClientException {
        this(prefix, hostname, port, 1000, constantTags, errorHandler);
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
     * @param waitResolution
     *@param constantTags tags to be added to all content sent
     * @param errorHandler handler to use when an exception occurs during usage, may be null to
 * indicate noop   @throws StatsDClientException if the client could not be started
     */
    public ConcurrentStatsDClient(final String prefix, final String hostname, final int port,
        int waitResolution, final String[] constantTags,
        final StatsDClientErrorHandler errorHandler)
        throws StatsDClientException {
        this(prefix, waitResolution, constantTags, errorHandler, staticStatsDAddressResolution
            (hostname, port));
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
    public ConcurrentStatsDClient(String prefix, String[] constantTags,
        StatsDClientErrorHandler errorHandler, Callable<InetSocketAddress> addressLookup) {
        this(prefix, 1000, constantTags, errorHandler, addressLookup);
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
     * @param waitResolution the time, in millis, the background IO thread waits before polling
     * the metric queue.
     * @param constantTags tags to be added to all content sent
     * @param errorHandler handler to use when an exception occurs during usage, may be null to
     * indicate noop
     * @param addressLookup yields the IP address and socket of the StatsD server
     * @throws StatsDClientException if the client could not be started
     */
    public ConcurrentStatsDClient(String prefix, int waitResolution, String[] constantTags,
        StatsDClientErrorHandler errorHandler, Callable<InetSocketAddress> addressLookup) {
        super(prefix, constantTags, errorHandler);

        queue = new ConcurrentLinkedQueue<>();
        executor.submit(new QueueConsumer(addressLookup));
        this.waitResolution = waitResolution;
    }

    @Override
    protected void send(String message) {
        queue.offer(message);
    }

    private class QueueConsumer implements Runnable {

        private final Sender sender;

        QueueConsumer(final Callable<InetSocketAddress> addressLookup) {
            sender = new Sender(addressLookup);
        }

        @Override
        public void run() {
            // Ensure that even if the executor/client is stopped, we send all accumulated metric
            // before stopping the background IO Thread.
            while (!executor.isShutdown() || !queue.isEmpty()) {
                try {
                    final String message = queue.poll();
                    if (message == null) {
                        Thread.sleep(waitResolution);
                        continue;
                    }
                    sender.addToBuffer(message);
                    if (null == queue.peek()) {
                        sender.blockingSend();
                    }
                } catch (final Exception e) {
                    handler.handle(e);
                }
            }
        }
    }
}
