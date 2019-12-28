package com.timgroup.statsd;

import jnr.unixsocket.UnixDatagramChannel;
import jnr.unixsocket.UnixSocketOptions;
import jnr.unixsocket.UnixSocketAddress;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Locale;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;


/**
 * A simple StatsD client implementation facilitating metrics recording.
 *
 * <p>Upon instantiation, this client will establish a socket connection to a StatsD instance
 * running on the specified host and port. Metrics are then sent over this connection as they are
 * received by the client.
 * </p>
 *
 * <p>Three key methods are provided for the submission of data-points for the application under
 * scrutiny:
 * <ul>
 *   <li>{@link #incrementCounter} - adds one to the value of the specified named counter</li>
 *   <li>{@link #recordGaugeValue} - records the latest fixed value for the specified named gauge</li>
 *   <li>{@link #recordExecutionTime} - records an execution time in milliseconds for the specified named operation</li>
 *   <li>{@link #recordHistogramValue} - records a value, to be tracked with average, maximum, and percentiles</li>
 *   <li>{@link #recordEvent} - records an event</li>
 *   <li>{@link #recordSetValue} - records a value in a set</li>
 * </ul>
 * From the perspective of the application, these methods are non-blocking, with the resulting
 * IO operations being carried out in a separate thread. Furthermore, these methods are guaranteed
 * not to throw an exception which may disrupt application execution.
 *
 * <p>As part of a clean system shutdown, the {@link #stop()} method should be invoked
 * on any StatsD clients.</p>
 *
 * @author Tom Denley
 *
 */
public class NonBlockingStatsDClient implements StatsDClient {

    public static final String DD_DOGSTATSD_PORT_ENV_VAR = "DD_DOGSTATSD_PORT";
    public static final String DD_AGENT_HOST_ENV_VAR = "DD_AGENT_HOST";
    public static final String DD_ENTITY_ID_ENV_VAR = "DD_ENTITY_ID";

    public static final int DEFAULT_MAX_PACKET_SIZE_BYTES = 1400;
    public static final int DEFAULT_QUEUE_SIZE = 4096;
    public static final int DEFAULT_POOL_SIZE = 512;
    public static final int DEFAULT_SENDER_WORKERS = 1;
    public static final int DEFAULT_DOGSTATSD_PORT = 8125;
    public static final int SOCKET_TIMEOUT_MS = 100;
    public static final int SOCKET_BUFFER_BYTES = -1;

    private static final StatsDClientErrorHandler NO_OP_HANDLER = new StatsDClientErrorHandler() {
        @Override public void handle(final Exception e) { /* No-op */ }
    };

    /**
     * Because NumberFormat is not thread-safe we cannot share instances across threads. Use a ThreadLocal to
     * create one pre thread as this seems to offer a significant performance improvement over creating one per-thread:
     * http://stackoverflow.com/a/1285297/2648
     * https://github.com/indeedeng/java-dogstatsd-client/issues/4
     */
    private static final ThreadLocal<NumberFormat> NUMBER_FORMATTERS = new ThreadLocal<NumberFormat>() {
        @Override
        protected NumberFormat initialValue() {

            // Always create the formatter for the US locale in order to avoid this bug:
            // https://github.com/indeedeng/java-dogstatsd-client/issues/3
            final NumberFormat numberFormatter = NumberFormat.getInstance(Locale.US);
            numberFormatter.setGroupingUsed(false);
            numberFormatter.setMaximumFractionDigits(6);

            // we need to specify a value for Double.NaN that is recognized by dogStatsD
            if (numberFormatter instanceof DecimalFormat) { // better safe than a runtime error
                final DecimalFormat decimalFormat = (DecimalFormat) numberFormatter;
                final DecimalFormatSymbols symbols = decimalFormat.getDecimalFormatSymbols();
                symbols.setNaN("NaN");
                decimalFormat.setDecimalFormatSymbols(symbols);
            }

            return numberFormatter;
        }
    };

    private static final ThreadLocal<NumberFormat> SAMPLE_RATE_FORMATTERS = new ThreadLocal<NumberFormat>() {
        @Override
        protected NumberFormat initialValue() {
            final NumberFormat numberFormatter = NumberFormat.getInstance(Locale.US);
            numberFormatter.setGroupingUsed(false);
            numberFormatter.setMinimumFractionDigits(6);

            if (numberFormatter instanceof DecimalFormat) {
                final DecimalFormat decimalFormat = (DecimalFormat) numberFormatter;
                final DecimalFormatSymbols symbols = decimalFormat.getDecimalFormatSymbols();
                symbols.setNaN("NaN");
                decimalFormat.setDecimalFormatSymbols(symbols);
            }
            return numberFormatter;
        }
    };

    private final String prefix;
    private final DatagramChannel clientChannel;
    private final StatsDClientErrorHandler handler;
    private final String constantTagsRendered;

    private final ExecutorService executor = Executors.newFixedThreadPool(2, new ThreadFactory() {
        final ThreadFactory delegate = Executors.defaultThreadFactory();
        @Override public Thread newThread(final Runnable r) {
            final Thread result = delegate.newThread(r);
            result.setName("StatsD-" + result.getName());
            result.setDaemon(true);
            return result;
        }
    });

    protected final StatsDProcessor statsDProcessor;
    protected final StatsDSender statsDSender;

    private final String ENTITY_ID_TAG_NAME = "dd.internal.entity_id" ;

    /**
     * Create a new StatsD client communicating with a StatsD instance on the
     * specified host and port. All messages send via this client will have
     * their keys prefixed with the specified string. The new client will
     * attempt to open a connection to the StatsD server immediately upon
     * instantiation, and may throw an exception if that a connection cannot
     * be established. Once a client has been instantiated in this way, all
     * exceptions thrown during subsequent usage are passed to the specified
     * handler and then consumed, guaranteeing that failures in metrics will
     * not affect normal code execution.
     *
     * @param prefix
     *     the prefix to apply to keys sent via this client
     * @param constantTags
     *     tags to be added to all content sent
     * @param errorHandler
     *     handler to use when an exception occurs during usage, may be null to indicate noop
     * @param addressLookup
     *     yields the IP address and socket of the StatsD server
     * @param queueSize
     *     the maximum amount of unprocessed messages in the Queue.
     * @param timeout
     *     the timeout in milliseconds for blocking operations. Applies to unix sockets only.
     * @param bufferSize
     *     the socket buffer size in bytes. Applies to unix sockets only.
     * @param maxPacketSizeBytes
     *     the maximum number of bytes for a message that can be sent
     * @param entityID
     *     the entity id value used with an internal tag for tracking client entity.
     *     If "entityID=null" the client default the value with the environment variable "DD_ENTITY_ID".
     *     If the environment variable is not defined, the internal tag is not added.
     * @param senderWorkers
     *     The number of sender worker threads submitting buffers to the socket.
     * @throws StatsDClientException
     *     if the client could not be started
     */
    public NonBlockingStatsDClient(final String prefix,  final int queueSize, String[] constantTags, final StatsDClientErrorHandler errorHandler,
                                   Callable<SocketAddress> addressLookup, final int timeout, final int bufferSize, final int maxPacketSizeBytes,
                                   String entityID, final int poolSize, final int senderWorkers, boolean blocking) throws StatsDClientException {
        if((prefix != null) && (!prefix.isEmpty())) {
            this.prefix = new StringBuilder(prefix).append(".").toString();
        } else {
            this.prefix = "";
        }
        if(errorHandler == null) {
            handler = NO_OP_HANDLER;
        }
        else {
            handler = errorHandler;
        }

        /* Empty list should be null for faster comparison */
        if((constantTags != null) && (constantTags.length == 0)) {
            constantTags = null;
        }

        // Support "dd.internal.entity_id" internal tag.
        constantTags = this.updateTagsWithEntityID(constantTags, entityID);
        if(constantTags != null) {
            constantTagsRendered = tagString(constantTags, null);
        } else {
            constantTagsRendered = null;
        }

        try {
            final SocketAddress address = addressLookup.call();
            if (address instanceof UnixSocketAddress) {
                clientChannel = UnixDatagramChannel.open();
                // Set send timeout, to handle the case where the transmission buffer is full
                // If no timeout is set, the send becomes blocking
                if (timeout > 0) {
                    clientChannel.setOption(UnixSocketOptions.SO_SNDTIMEO, timeout);
                }
                if (bufferSize > 0) {
                    clientChannel.setOption(UnixSocketOptions.SO_SNDBUF, bufferSize);
                }
            } else{
                clientChannel = DatagramChannel.open();
            }

            statsDProcessor = createProcessor(queueSize, handler, maxPacketSizeBytes, poolSize, blocking);
            statsDSender = createSender(addressLookup, handler, clientChannel,
                    statsDProcessor.getBufferPool(), statsDProcessor.getOutboundQueue(), senderWorkers);

        } catch (final Exception e) {
            throw new StatsDClientException("Failed to start StatsD client", e);
        }

        executor.submit(statsDProcessor);
        executor.submit(statsDSender);
    }

    protected StatsDProcessor createProcessor(final int queueSize, final StatsDClientErrorHandler handler,
            final int maxPacketSizeBytes, final int bufferPoolSize, boolean blocking) throws Exception {
        if (blocking) {
            return new StatsDBlockingProcessor(queueSize, handler, maxPacketSizeBytes, bufferPoolSize);
        } else {
            return new StatsDNonBlockingProcessor(queueSize, handler, maxPacketSizeBytes, bufferPoolSize);
        }
    }

    protected StatsDSender createSender(final Callable<SocketAddress> addressLookup, final StatsDClientErrorHandler handler,
            final DatagramChannel clientChannel, BufferPool pool, BlockingQueue<ByteBuffer> buffers, final int senderWorkers) throws Exception {
        return new StatsDSender(addressLookup, clientChannel, handler, pool, buffers, senderWorkers);
    }

    /**
     * Cleanly shut down this StatsD client. This method may throw an exception if
     * the socket cannot be closed.
     */
    @Override
    public void stop() {
        try {
            statsDProcessor.shutdown();
            statsDSender.shutdown();
            executor.shutdown();
            try {
                executor.awaitTermination(30, TimeUnit.SECONDS);
                if (!executor.isTerminated()) {
                    executor.shutdownNow();
                }
            } catch (Exception e) {
                handler.handle(e);
                if (!executor.isTerminated()) {
                    executor.shutdownNow();
                }
            }
        }
        catch (final Exception e) {
            handler.handle(e);
        }
        finally {
            if (clientChannel != null) {
                try {
                    clientChannel.close();
                }
                catch (final IOException e) {
                    handler.handle(e);
                }
            }
        }
    }

    @Override
    public void close() {
        stop();
    }

    /**
     * Generate a suffix conveying the given tag list to the client
     */
    static String tagString(final String[] tags, final String tagPrefix) {
        final StringBuilder sb;
        if(tagPrefix != null) {
            if((tags == null) || (tags.length == 0)) {
                return tagPrefix;
            }
            sb = new StringBuilder(tagPrefix);
            sb.append(",");
        } else {
            if((tags == null) || (tags.length == 0)) {
                return "";
            }
            sb = new StringBuilder("|#");
        }

        for(int n=tags.length - 1; n>=0; n--) {
            sb.append(tags[n]);
            if(n > 0) {
                sb.append(",");
            }
        }
        return sb.toString();
    }

    /**
     * Generate a suffix conveying the given tag list to the client
     */
    String tagString(final String[] tags) {
        return tagString(tags, constantTagsRendered);
    }

    /**
     * Adjusts the specified counter by a given delta.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
     *
     * @param aspect
     *     the name of the counter to adjust
     * @param delta
     *     the amount to adjust the counter by
     * @param tags
     *     array of tags to be added to the data
     */
    @Override
    public void count(final String aspect, final long delta, final String... tags) {
        send(new StringBuilder(prefix).append(aspect).append(":").append(delta).append("|c").append(tagString(tags)).toString());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void count(final String aspect, final long delta, final double sampleRate, final String...tags) {
    	if(isInvalidSample(sampleRate)) {
    		return;
    	}
        send(new StringBuilder(prefix).append(aspect).append(":").append(delta).append("|c|@").append(SAMPLE_RATE_FORMATTERS.get().format(sampleRate)).append(tagString(tags)).toString());
    }

    /**
     * Adjusts the specified counter by a given delta.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
     *
     * @param aspect
     *     the name of the counter to adjust
     * @param delta
     *     the amount to adjust the counter by
     * @param tags
     *     array of tags to be added to the data
     */
    @Override
    public void count(final String aspect, final double delta, final String... tags) {
        send(new StringBuilder(prefix).append(aspect).append(":").append(NUMBER_FORMATTERS.get().format(delta)).append("|c").append(tagString(tags)).toString());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void count(final String aspect, final double delta, final double sampleRate, final String...tags) {
        if(isInvalidSample(sampleRate)) {
            return;
        }
        send(new StringBuilder(prefix).append(aspect).append(":").append(NUMBER_FORMATTERS.get().format(delta)).append("|c|@").append(SAMPLE_RATE_FORMATTERS.get().format(sampleRate)).append(tagString(tags)).toString());
    }

    /**
     * Increments the specified counter by one.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
     *
     * @param aspect
     *     the name of the counter to increment
     * @param tags
     *     array of tags to be added to the data
     */
    @Override
    public void incrementCounter(final String aspect, final String... tags) {
        count(aspect, 1, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void incrementCounter(final String aspect, final double sampleRate, final String... tags) {
    	count(aspect, 1, sampleRate, tags);
    }

    /**
     * Convenience method equivalent to {@link #incrementCounter(String, String[])}.
     */
    @Override
    public void increment(final String aspect, final String... tags) {
        incrementCounter(aspect, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void increment(final String aspect, final double sampleRate, final String...tags ) {
    	incrementCounter(aspect, sampleRate, tags);
    }

    /**
     * Decrements the specified counter by one.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
     *
     * @param aspect
     *     the name of the counter to decrement
     * @param tags
     *     array of tags to be added to the data
     */
    @Override
    public void decrementCounter(final String aspect, final String... tags) {
        count(aspect, -1, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void decrementCounter(String aspect, final double sampleRate, final String... tags) {
        count(aspect, -1, sampleRate, tags);
    }

    /**
     * Convenience method equivalent to {@link #decrementCounter(String, String[])}.
     */
    @Override
    public void decrement(final String aspect, final String... tags) {
        decrementCounter(aspect, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void decrement(final String aspect, final double sampleRate, final String... tags) {
        decrementCounter(aspect, sampleRate, tags);
    }

    /**
     * Records the latest fixed value for the specified named gauge.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
     *
     * @param aspect
     *     the name of the gauge
     * @param value
     *     the new reading of the gauge
     * @param tags
     *     array of tags to be added to the data
     */
    @Override
    public void recordGaugeValue(final String aspect, final double value, final String... tags) {
        /* Intentionally using %s rather than %f here to avoid
         * padding with extra 0s to represent precision */
        send(new StringBuilder(prefix).append(aspect).append(":").append(NUMBER_FORMATTERS.get().format(value)).append("|g").append(tagString(tags)).toString());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void recordGaugeValue(final String aspect, final double value, final double sampleRate, final String... tags) {
    	if(isInvalidSample(sampleRate)) {
    		return;
    	}
        send(new StringBuilder(prefix).append(aspect).append(":").append(NUMBER_FORMATTERS.get().format(value)).append("|g|@").append(SAMPLE_RATE_FORMATTERS.get().format(sampleRate)).append(tagString(tags)).toString());
    }

    /**
     * Convenience method equivalent to {@link #recordGaugeValue(String, double, String[])}.
     */
    @Override
    public void gauge(final String aspect, final double value, final String... tags) {
        recordGaugeValue(aspect, value, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void gauge(final String aspect, final double value, final double sampleRate, final String... tags) {
        recordGaugeValue(aspect, value, sampleRate, tags);
    }


    /**
     * Records the latest fixed value for the specified named gauge.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
     *
     * @param aspect
     *     the name of the gauge
     * @param value
     *     the new reading of the gauge
     * @param tags
     *     array of tags to be added to the data
     */
    @Override
    public void recordGaugeValue(final String aspect, final long value, final String... tags) {
        send(new StringBuilder(prefix).append(aspect).append(":").append(value).append("|g").append(tagString(tags)).toString());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void recordGaugeValue(final String aspect, final long value, final double sampleRate, final String... tags) {
    	if(isInvalidSample(sampleRate)) {
    		return;
    	}
        send(new StringBuilder(prefix).append(aspect).append(":").append(value).append("|g|@").append(SAMPLE_RATE_FORMATTERS.get().format(sampleRate)).append(tagString(tags)).toString());
    }

    /**
     * Convenience method equivalent to {@link #recordGaugeValue(String, long, String[])}.
     */
    @Override
    public void gauge(final String aspect, final long value, final String... tags) {
        recordGaugeValue(aspect, value, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void gauge(final String aspect, final long value, final double sampleRate, final String... tags) {
        recordGaugeValue(aspect, value, sampleRate, tags);
    }

    /**
     * Records an execution time in milliseconds for the specified named operation.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
     *
     * @param aspect
     *     the name of the timed operation
     * @param timeInMs
     *     the time in milliseconds
     * @param tags
     *     array of tags to be added to the data
     */
    @Override
    public void recordExecutionTime(final String aspect, final long timeInMs, final String... tags) {
        send(new StringBuilder(prefix).append(aspect).append(":").append(timeInMs).append("|ms").append(tagString(tags)).toString());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void recordExecutionTime(final String aspect, final long timeInMs, final double sampleRate, final String... tags) {
    	if(isInvalidSample(sampleRate)) {
    		return;
    	}
        send(new StringBuilder(prefix).append(aspect).append(":").append(timeInMs).append("|ms|@").append(SAMPLE_RATE_FORMATTERS.get().format(sampleRate)).append(tagString(tags)).toString());
    }

    /**
     * Convenience method equivalent to {@link #recordExecutionTime(String, long, String[])}.
     */
    @Override
    public void time(final String aspect, final long value, final String... tags) {
        recordExecutionTime(aspect, value, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void time(final String aspect, final long value, final double sampleRate, final String... tags) {
        recordExecutionTime(aspect, value, sampleRate, tags);
    }

    /**
     * Records a value for the specified named histogram.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
     *
     * @param aspect
     *     the name of the histogram
     * @param value
     *     the value to be incorporated in the histogram
     * @param tags
     *     array of tags to be added to the data
     */
    @Override
    public void recordHistogramValue(final String aspect, final double value, final String... tags) {
        /* Intentionally using %s rather than %f here to avoid
         * padding with extra 0s to represent precision */
        send(new StringBuilder(prefix).append(aspect).append(":").append(NUMBER_FORMATTERS.get().format(value)).append("|h").append(tagString(tags)).toString());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void recordHistogramValue(final String aspect, final double value, final double sampleRate, final String... tags) {
    	if(isInvalidSample(sampleRate)) {
    		return;
    	}
    	  /* Intentionally using %s rather than %f here to avoid
    	   * padding with extra 0s to represent precision */
        send(new StringBuilder(prefix).append(aspect).append(":").append(NUMBER_FORMATTERS.get().format(value)).append("|h|@").append(SAMPLE_RATE_FORMATTERS.get().format(sampleRate)).append(tagString(tags)).toString());
    }

    /**
     * Convenience method equivalent to {@link #recordHistogramValue(String, double, String[])}.
     */
    @Override
    public void histogram(final String aspect, final double value, final String... tags) {
        recordHistogramValue(aspect, value, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void histogram(final String aspect, final double value, final double sampleRate, final String... tags) {
        recordHistogramValue(aspect, value, sampleRate, tags);
    }

    /**
     * Records a value for the specified named histogram.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
     *
     * @param aspect
     *     the name of the histogram
     * @param value
     *     the value to be incorporated in the histogram
     * @param tags
     *     array of tags to be added to the data
     */
    @Override
    public void recordHistogramValue(final String aspect, final long value, final String... tags) {
        send(new StringBuilder(prefix).append(aspect).append(":").append(value).append("|h").append(tagString(tags)).toString());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void recordHistogramValue(final String aspect, final long value, final double sampleRate, final String... tags) {
    	if(isInvalidSample(sampleRate)) {
    		return;
    	}
        send(new StringBuilder(prefix).append(aspect).append(":").append(value).append("|h|@").append(SAMPLE_RATE_FORMATTERS.get().format(sampleRate)).append(tagString(tags)).toString());
    }

    /**
     * Convenience method equivalent to {@link #recordHistogramValue(String, long, String[])}.
     */
    @Override
    public void histogram(final String aspect, final long value, final String... tags) {
        recordHistogramValue(aspect, value, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void histogram(final String aspect, final long value, final double sampleRate, final String... tags) {
        recordHistogramValue(aspect, value, sampleRate, tags);
    }

     /**
     * Records a value for the specified named distribution.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
     *
     * <p>This is a beta feature and must be enabled specifically for your organization.</p>
     *
     * @param aspect
     *     the name of the distribution
     * @param value
     *     the value to be incorporated in the distribution
     * @param tags
     *     array of tags to be added to the data
     */
    @Override
    public void recordDistributionValue(final String aspect, final double value, final String... tags) {
        /* Intentionally using %s rather than %f here to avoid
         * padding with extra 0s to represent precision */
        send(new StringBuilder(prefix).append(aspect).append(":").append(NUMBER_FORMATTERS.get().format(value)).append("|d").append(tagString(tags)).toString());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void recordDistributionValue(final String aspect, final double value, final double sampleRate, final String... tags) {
    	if(isInvalidSample(sampleRate)) {
    		return;
    	}
    	  /* Intentionally using %s rather than %f here to avoid
    	   * padding with extra 0s to represent precision */
        send(new StringBuilder(prefix).append(aspect).append(":").append(NUMBER_FORMATTERS.get().format(value)).append("|d|@").append(SAMPLE_RATE_FORMATTERS.get().format(sampleRate)).append(tagString(tags)).toString());
    }

    /**
     * Convenience method equivalent to {@link #recordDistributionValue(String, double, String[])}.
     */
    @Override
    public void distribution(final String aspect, final double value, final String... tags) {
        recordDistributionValue(aspect, value, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void distribution(final String aspect, final double value, final double sampleRate, final String... tags) {
        recordDistributionValue(aspect, value, sampleRate, tags);
    }
    /**
     * Records a value for the specified named distribution.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
     *
     * <p>This is a beta feature and must be enabled specifically for your organization.</p>
     *
     * @param aspect
     *     the name of the distribution
     * @param value
     *     the value to be incorporated in the distribution
     * @param tags
     *     array of tags to be added to the data
     */
    @Override
    public void recordDistributionValue(final String aspect, final long value, final String... tags) {
        send(new StringBuilder(prefix).append(aspect).append(":").append(value).append("|d").append(tagString(tags)).toString());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void recordDistributionValue(final String aspect, final long value, final double sampleRate, final String... tags) {
    	if(isInvalidSample(sampleRate)) {
    		return;
    	}
        send(new StringBuilder(prefix).append(aspect).append(":").append(value).append("|d|@").append(SAMPLE_RATE_FORMATTERS.get().format(sampleRate)).append(tagString(tags)).toString());
    }

    /**
     * Convenience method equivalent to {@link #recordDistributionValue(String, long, String[])}.
     */
    @Override
    public void distribution(final String aspect, final long value, final String... tags) {
        recordDistributionValue(aspect, value, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void distribution(final String aspect, final long value, final double sampleRate, final String... tags) {
        recordDistributionValue(aspect, value, sampleRate, tags);
    }

    private String eventMap(final Event event) {
        final StringBuilder res = new StringBuilder("");

        final long millisSinceEpoch = event.getMillisSinceEpoch();
        if (millisSinceEpoch != -1) {
            res.append("|d:").append(millisSinceEpoch / 1000);
        }

        final String hostname = event.getHostname();
        if (hostname != null) {
            res.append("|h:").append(hostname);
        }

        final String aggregationKey = event.getAggregationKey();
        if (aggregationKey != null) {
            res.append("|k:").append(aggregationKey);
        }

        final String priority = event.getPriority();
        if (priority != null) {
            res.append("|p:").append(priority);
        }

        final String alertType = event.getAlertType();
        if (alertType != null) {
            res.append("|t:").append(alertType);
        }

        return res.toString();
    }

    /**
     * Records an event
     *
     * <p>This method is a DataDog extension, and may not work with other servers.</p>
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
     *
     * @param event
     *     The event to record
     * @param tags
     *     array of tags to be added to the data
     *
     * @see <a href="http://docs.datadoghq.com/guides/dogstatsd/#events-1">http://docs.datadoghq.com/guides/dogstatsd/#events-1</a>
     */
    @Override
    public void recordEvent(final Event event, final String... tags) {
        final String title = escapeEventString(prefix + event.getTitle());
        final String text = escapeEventString(event.getText());
        send(new StringBuilder("_e{").append(title.length()).append(",").append(text.length()).append("}:").append(title)
                .append("|").append(text).append(eventMap(event)).append(tagString(tags)).toString());
    }

    private String escapeEventString(final String title) {
        return title.replace("\n", "\\n");
    }

    /**
     * Records a run status for the specified named service check.
     *
     * <p>This method is a DataDog extension, and may not work with other servers.</p>
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
     *
     * @param sc
     *     the service check object
     */
    @Override
    public void recordServiceCheckRun(final ServiceCheck sc) {
        send(toStatsDString(sc));
    }

    /**
     * Updates and returns tags completed with the entityID tag if needed.
     *
     * @param tags the current constant tags array
     *
     * @param entityID the entityID string provided by argument
     *
     * @return array of tags
     */
    private String[] updateTagsWithEntityID(String[] tags, String entityID) {
        // Support "dd.internal.entity_id" internal tag.
        if(entityID == null || entityID.trim().isEmpty()) {
            // if the entityID parameter is null, default to the environment variable
            entityID = System.getenv(DD_ENTITY_ID_ENV_VAR);
        }
        if(entityID != null && !entityID.trim().isEmpty()) {
            final String entityTag = new StringBuilder(ENTITY_ID_TAG_NAME).append(":").append(entityID).toString();
            if (tags == null) {
                tags = new String[]{entityTag};
            } else {
                tags = Arrays.copyOf(tags, tags.length+1);
                // Now that tags is one element longer, tags.length has changed...
                tags[tags.length - 1] = entityTag;
            }
        }
        return tags;
    }

    private String toStatsDString(final ServiceCheck sc) {
        // see http://docs.datadoghq.com/guides/dogstatsd/#service-checks
        final StringBuilder sb = new StringBuilder();
        sb.append("_sc|").append(sc.getName()).append("|").append(sc.getStatus());
        if (sc.getTimestamp() > 0) {
            sb.append("|d:").append(sc.getTimestamp());
        }
        if (sc.getHostname() != null) {
            sb.append("|h:").append(sc.getHostname());
        }
        sb.append(tagString(sc.getTags()));
        if (sc.getMessage() != null) {
            sb.append("|m:").append(sc.getEscapedMessage());
        }
        return sb.toString();
    }

    /**
     * Convenience method equivalent to {@link #recordServiceCheckRun(ServiceCheck sc)}.
     */
    @Override
    public void serviceCheck(final ServiceCheck sc) {
        recordServiceCheckRun(sc);
    }


    /**
     * Records a value for the specified set.
     *
     * Sets are used to count the number of unique elements in a group. If you want to track the number of
     * unique visitor to your site, sets are a great way to do that.
     *
     * <p>This method is a DataDog extension, and may not work with other servers.</p>
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
     *
     * @param aspect
     *     the name of the set
     * @param value
     *     the value to track
     * @param tags
     *     array of tags to be added to the data
     *
     * @see <a href="http://docs.datadoghq.com/guides/dogstatsd/#sets">http://docs.datadoghq.com/guides/dogstatsd/#sets</a>
     */
    @Override
    public void recordSetValue(final String aspect, final String value, final String... tags) {
        // documentation is light, but looking at dogstatsd source, we can send string values
        // here instead of numbers
        send(new StringBuilder(prefix).append(aspect).append(":").append(value).append("|s").append(tagString(tags)).toString());
    }

    private void send(final String message) {
        statsDProcessor.send(message);
    }

    private boolean isInvalidSample(double sampleRate) {
        return sampleRate != 1 && ThreadLocalRandom.current().nextDouble() > sampleRate;
    }


}
