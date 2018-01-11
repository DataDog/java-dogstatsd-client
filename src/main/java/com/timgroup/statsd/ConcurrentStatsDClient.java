package com.timgroup.statsd;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * A StatsDClient implementation for usage in highly threaded situation.
 * <p> Under high load, the {@link java.util.concurrent.BlockingQueue} used by
 * {@link NonBlockingStatsDClient} for inter-thread communication can become a contention point,
 * since multiple thread are trying to obtain the Write Lock on the queue to insert metrics
 * elements. With the {@link ConcurrentStatsDClient}, it uses a non-blocking, lock-free queue for
 * inter-thread communication which remove that contention point. However, since this is a
 * non-blocking queue, the background thread needs to wait for element to come into the queue
 * using a separate mechanism; currently it  uses {@link Thread#sleep(long)} to achieve this,
 * which may hurt metric reporting and responsiveness. </p>
 * <p> {@link NonBlockingStatsDClient} will perform better than {@link ConcurrentStatsDClient}
 * under low to moderate load, but {@link ConcurrentStatsDClient} outperforms
 * {@link NonBlockingStatsDClient} under moderate to high load. Proper benchmarking by the client
 * application needs to be done to decide which implementation to choose from.</p>
 * <p>Thread-safety note: this class is safe to use by multiple thread without external
 * synchronization.</p>
 *
 * @see NonBlockingStatsDClient
 *
 * @author Pascal Gélinas
 */
public final class ConcurrentStatsDClient extends BackgroundStatsDClient {

    private final ConcurrentLinkedQueue<String> queue;
    private final long waitResolution;

    /**
     * Create a new StatsD client communicating with a StatsD instance on the specified host and
     * port. All messages send via this client will have their keys prefixed with the specified
     * string. The new client will attempt to open a connection to the StatsD server immediately
     * upon instantiation, and may throw an exception if that a connection cannot be established.
     * Once a client has been instantiated in this way, all exceptions thrown during subsequent
     * usage are consumed, guaranteeing that failures in metrics will not affect normal code
     * execution.
     * <p>The client will use the UDP protcol to communicate with the StatsD instance.</p>
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
    public ConcurrentStatsDClient(String prefix, String hostname, int port) {
        this(prefix, hostname, port, 1000);
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
     * @param waitResolution
     *     the time, in millis, the background IO thread waits before polling the metric queue.
     * @throws StatsDClientException
     *     if the client could not be started
     */
    public ConcurrentStatsDClient(String prefix, String hostname, int port, long waitResolution) {
        this(prefix, waitResolution, null, null,
            createStatsDProtocol(hostname, port));
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
     * @param waitResolution
     *     the time, in millis, the background IO thread waits before polling the metric queue.
     * @param constantTags
     *     tags to be added to all content sent
     * @param errorHandler
     *     handler to use when an exception occurs during usage, may be null to indicate noop
     * @param protocol
     *     the underlying protocol to use for communication.
     * @throws StatsDClientException
     *     if the client could not be started
     */
    public ConcurrentStatsDClient(String prefix, long waitResolution, String[] constantTags,
        StatsDClientErrorHandler errorHandler, Protocol protocol) {
        super(prefix, constantTags, errorHandler);

        queue = new ConcurrentLinkedQueue<>();
        executor.submit(new QueueConsumer(protocol));
        this.waitResolution = waitResolution;
    }

    @Override
    protected void send(String message) {
        queue.offer(message);
    }

    private class QueueConsumer implements Runnable {

        private final Protocol protocol;

        QueueConsumer(Protocol protocol) {
            this.protocol = protocol;
        }

        @Override
        public void run() {
            // Ensure that even if the executor/client is stopped, we send all accumulated metric
            // before stopping the background IO Thread.
            while (!executor.isShutdown() || !queue.isEmpty()) {
                try {
                    final String message = queue.poll();
                    if (null == message) {
                        Thread.sleep(waitResolution);
                        continue;
                    }
                    protocol.send(message);
                    if (null == queue.peek()) {
                        protocol.flush();
                    }
                } catch (final Exception e) {
                    handler.handle(e);
                }
            }
        }
    }
}
