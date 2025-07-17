package com.timgroup.statsd;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;

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
 * <p>Some methods allow recording a value for a specific point in time by taking an extra
 * timestamp parameter. Such values are exempt from aggregation and the value should indicate the
 * final metric value at the given time. Please refer to Datadog documentation for the range of
 * accepted timestamp values.
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
    public static final String DD_NAMED_PIPE_ENV_VAR = "DD_DOGSTATSD_PIPE_NAME";
    public static final String DD_ENTITY_ID_ENV_VAR = "DD_ENTITY_ID";
    private static final String ENTITY_ID_TAG_NAME = "dd.internal.entity_id" ;
    public static final String ORIGIN_DETECTION_ENABLED_ENV_VAR = "DD_ORIGIN_DETECTION_ENABLED";
    public static final String DD_DOGSTATSD_URL_ENV_VAR = "DD_DOGSTATSD_URL";

    private static final long MIN_TIMESTAMP = 1;

    enum Literal {
        SERVICE,
        ENV,
        VERSION
        ;
        private static final String PREFIX = "dd";
        String envName() {
            return (PREFIX + "_" + toString()).toUpperCase();
        }

        String envVal() {
            return System.getenv(envName());
        }

        String tag() {
            return toString().toLowerCase();
        }
    }

    public static final int DEFAULT_UDP_MAX_PACKET_SIZE_BYTES = 1432;
    public static final int DEFAULT_UDS_MAX_PACKET_SIZE_BYTES = 8192;
    public static final int DEFAULT_QUEUE_SIZE = 4096;
    public static final int DEFAULT_POOL_SIZE = 512;
    public static final int DEFAULT_PROCESSOR_WORKERS = 1;
    public static final int DEFAULT_SENDER_WORKERS = 1;
    public static final int DEFAULT_DOGSTATSD_PORT = 8125;
    public static final int SOCKET_TIMEOUT_MS = 100;
    public static final int SOCKET_BUFFER_BYTES = -1;
    public static final boolean DEFAULT_BLOCKING = false;
    public static final boolean DEFAULT_ENABLE_TELEMETRY = true;

    public static final boolean DEFAULT_ENABLE_AGGREGATION = true;
    public static final boolean DEFAULT_ENABLE_ORIGIN_DETECTION = true;
    public static final int SOCKET_CONNECT_TIMEOUT_MS = 1000;

    public static final String CLIENT_TAG = "client:java";
    public static final String CLIENT_VERSION_TAG = "client_version:";
    public static final String CLIENT_TRANSPORT_TAG = "client_transport:";


    /**
     * UTF-8 is the expected encoding for data sent to the agent.
     */
    public static final Charset UTF_8 = StandardCharsets.UTF_8;

    private static final StatsDClientErrorHandler NO_OP_HANDLER = new StatsDClientErrorHandler() {
        @Override public void handle(final Exception ex) { /* No-op */ }
    };

    /**
     * The NumberFormat instances are not threadsafe and thus defined as ThreadLocal
     * for safety.
     */
    protected static final ThreadLocal<NumberFormat> NUMBER_FORMATTER = new ThreadLocal<NumberFormat>() {
        @Override
        protected NumberFormat initialValue() {
            return newFormatter(false);
        }
    };
    protected static final ThreadLocal<NumberFormat> SAMPLE_RATE_FORMATTER = new ThreadLocal<NumberFormat>() {
        @Override
        protected NumberFormat initialValue() {
            return newFormatter(true);
        }
    };

    static {
    }

    private static NumberFormat newFormatter(boolean sampler) {
        // Always create the formatter for the US locale in order to avoid this bug:
        // https://github.com/indeedeng/java-dogstatsd-client/issues/3
        NumberFormat numberFormatter = NumberFormat.getInstance(Locale.US);
        numberFormatter.setGroupingUsed(false);

        // we need to specify a value for Double.NaN that is recognized by dogStatsD
        if (numberFormatter instanceof DecimalFormat) { // better safe than a runtime error
            final DecimalFormat decimalFormat = (DecimalFormat) numberFormatter;
            final DecimalFormatSymbols symbols = decimalFormat.getDecimalFormatSymbols();
            symbols.setNaN("NaN");
            decimalFormat.setDecimalFormatSymbols(symbols);
        }

        if (sampler) {
            numberFormatter.setMinimumFractionDigits(6);
        } else {
            numberFormatter.setMaximumFractionDigits(6);
        }

        return numberFormatter;
    }

    protected static String format(ThreadLocal<NumberFormat> formatter, Number value) {
        return formatter.get().format(value);
    }

    final String prefix;
    private final ClientChannel clientChannel;
    private final ClientChannel telemetryClientChannel;
    private final StatsDClientErrorHandler handler;
    private final String constantTagsRendered;

    // Typically the telemetry and regular processors will be the same,
    // but a separate destination for telemetry is supported.
    protected final StatsDProcessor statsDProcessor;
    protected StatsDProcessor telemetryStatsDProcessor;
    protected final StatsDSender statsDSender;
    protected StatsDSender telemetryStatsDSender;
    protected final Telemetry telemetry;
    final String telemetryTags;
    private final int maxPacketSizeBytes;
    private final boolean blocking;
    private final String containerID;
    private final String externalEnv = Utf8.sanitize(System.getenv("DD_EXTERNAL_ENV"));
    final TagsCardinality clientTagsCardinality;

    /**
     * Create a new StatsD client communicating with a StatsD instance on the
     * host and port specified by the given builder.
     * The builder must be resolved before calling this internal constructor.
     *
     * @param builder
     *     the resolved configuration builder
     *
     * @see NonBlockingStatsDClientBuilder#resolve()
     */
    public NonBlockingStatsDClient(final NonBlockingStatsDClientBuilder builder) throws StatsDClientException {

        if (builder.prefix != null && !builder.prefix.isEmpty()) {
            prefix = builder.prefix + ".";
        } else {
            prefix = "";
        }

        if (builder.errorHandler == null) {
            handler = NO_OP_HANDLER;
        } else {
            handler = builder.errorHandler;
        }

        blocking = builder.blocking;
        maxPacketSizeBytes = builder.maxPacketSizeBytes;
        clientTagsCardinality = builder.tagsCardinality;

        {
            List<String> costantPreTags = new ArrayList<>();
            if (builder.constantTags != null) {
                for (final String constantTag : builder.constantTags) {
                    costantPreTags.add(constantTag);
                }
            }
            // Support "dd.internal.entity_id" internal tag.
            updateTagsWithEntityID(costantPreTags, builder.entityID);
            for (final Literal literal : Literal.values()) {
                final String envVal = literal.envVal();
                if (envVal != null && !envVal.trim().isEmpty()) {
                    costantPreTags.add(literal.tag() + ":" + envVal);
                }
            }
            if (costantPreTags.isEmpty()) {
                constantTagsRendered = null;
            } else {
                constantTagsRendered = tagString(
                        costantPreTags.toArray(new String[costantPreTags.size()]), null, new StringBuilder()).toString();
            }
            costantPreTags = null;
            containerID = getContainerID(builder.containerID, builder.originDetectionEnabled);
        }

        try {
            clientChannel = createByteChannel(builder.addressLookup, builder.timeout, builder.connectionTimeout,
                builder.socketBufferSize);

            ThreadFactory threadFactory = builder.threadFactory != null ? builder.threadFactory : new StatsDThreadFactory();

            int aggregationFlushInterval =  builder.enableAggregation ? builder.aggregationFlushInterval : 0;
            statsDProcessor = createProcessor(builder.queueSize, handler, getPacketSize(clientChannel), builder.bufferPoolSize,
                builder.processorWorkers, builder.blocking, aggregationFlushInterval, builder.aggregationShards, threadFactory);

            Properties properties = new Properties();
            properties.load(getClass().getClassLoader().getResourceAsStream(
                "dogstatsd/version.properties"));

            telemetryTags = tagString(new String[]{CLIENT_TRANSPORT_TAG + clientChannel.getTransportType(),
                                                   CLIENT_VERSION_TAG + properties.getProperty("dogstatsd_client_version"),
                                                   CLIENT_TAG}, new StringBuilder()).toString();

            if (builder.addressLookup == builder.telemetryAddressLookup) {
                telemetryClientChannel = clientChannel;
                telemetryStatsDProcessor = statsDProcessor;
            } else {
                telemetryClientChannel = createByteChannel(builder.telemetryAddressLookup, builder.timeout,
                    builder.connectionTimeout, builder.socketBufferSize);

                // similar settings, but a single worker and non-blocking.
                telemetryStatsDProcessor = createProcessor(builder.queueSize, handler, getPacketSize(telemetryClientChannel),
                        builder.bufferPoolSize, 1, false, 0, builder.aggregationShards, threadFactory);
            }

            telemetry = new Telemetry(this);

            statsDSender = createSender(handler, clientChannel, statsDProcessor.getBufferPool(),
                    statsDProcessor.getOutboundQueue(), builder.senderWorkers, threadFactory);

            telemetryStatsDSender = statsDSender;
            if (telemetryStatsDProcessor != statsDProcessor) {
                // TODO: figure out why the hell telemetryClientChannel does not work here!
                telemetryStatsDSender = createSender(handler, telemetryClientChannel,
                        telemetryStatsDProcessor.getBufferPool(), telemetryStatsDProcessor.getOutboundQueue(),
                        1, threadFactory);
            }

            // set telemetry
            statsDProcessor.setTelemetry(telemetry);
            statsDSender.setTelemetry(telemetry);

        } catch (final Exception e) {
            throw new StatsDClientException("Failed to start StatsD client", e);
        }

        statsDProcessor.startWorkers("StatsD-Processor-");
        statsDSender.startWorkers("StatsD-Sender-");

        if (builder.enableTelemetry) {
            if (telemetryStatsDProcessor != statsDProcessor) {
                telemetryStatsDProcessor.startWorkers("StatsD-TelemetryProcessor-");
                telemetryStatsDSender.startWorkers("StatsD-TelemetrySender-");
            }
            telemetry.start(builder.telemetryFlushInterval);
        }
    }

    protected StatsDProcessor createProcessor(final int queueSize, final StatsDClientErrorHandler handler,
            final int maxPacketSizeBytes, final int bufferPoolSize, final int workers, final boolean blocking,
            final int aggregationFlushInterval, final int aggregationShards, final ThreadFactory threadFactory)
            throws Exception {
        if (blocking) {
            return new StatsDBlockingProcessor(queueSize, handler, maxPacketSizeBytes, bufferPoolSize,
                    workers, aggregationFlushInterval, aggregationShards, threadFactory);
        } else {
            return new StatsDNonBlockingProcessor(queueSize, handler, maxPacketSizeBytes, bufferPoolSize,
                    workers, aggregationFlushInterval, aggregationShards, threadFactory);
        }
    }

    protected StatsDSender createSender(final StatsDClientErrorHandler handler,
            final WritableByteChannel clientChannel, BufferPool pool, BlockingQueue<ByteBuffer> buffers, final int senderWorkers,
            final ThreadFactory threadFactory) throws Exception {
        return new StatsDSender(clientChannel, handler, pool, buffers, senderWorkers, threadFactory);
    }

    /**
     * Cleanly shut down this StatsD client. This method may throw an exception if
     * the socket cannot be closed.
     *
     * <p>In blocking mode, this will block until all messages are sent to the server.
     */
    @Override
    public void stop() {
        try {
            this.telemetry.stop();
            statsDProcessor.shutdown(blocking);
            statsDSender.shutdown(blocking);

            // shut down telemetry workers if need be
            if (telemetryStatsDProcessor != statsDProcessor) {
                telemetryStatsDProcessor.shutdown(false);
                telemetryStatsDSender.shutdown(false);
            }
        } catch (final Exception e) {
            handler.handle(e);
        } finally {
            if (clientChannel != null) {
                try {
                    clientChannel.close();
                } catch (final IOException e) {
                    handler.handle(e);
                }
            }

            if (telemetryClientChannel != null && telemetryClientChannel != clientChannel) {
                try {
                    telemetryClientChannel.close();
                } catch (final IOException e) {
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
     * Return tag list as a tag string.
     * Generate a suffix conveying the given tag list to the client
     */
    static StringBuilder tagString(final String[] tags, final String tagPrefix, final StringBuilder sb) {
        if (tagPrefix != null) {
            sb.append(tagPrefix);
            if ((tags == null) || (tags.length == 0)) {
                return sb;
            }
            sb.append(',');
        } else {
            if ((tags == null) || (tags.length == 0)) {
                return sb;
            }
            sb.append("|#");
        }

        for (int n = tags.length - 1; n >= 0; n--) {
            sb.append(tags[n]);
            if (n > 0) {
                sb.append(',');
            }
        }
        return sb;
    }

    /**
     * Generate a suffix conveying the given tag list to the client.
     */
    StringBuilder tagString(final String[] tags, StringBuilder builder) {
        return tagString(tags, constantTagsRendered, builder);
    }

    ClientChannel createByteChannel(
            Callable<SocketAddress> addressLookup, int timeout, int connectionTimeout, int bufferSize)
            throws Exception {
        final SocketAddress address = addressLookup.call();
        if (address instanceof NamedPipeSocketAddress) {
            return new NamedPipeClientChannel((NamedPipeSocketAddress) address);
        }
        if (address instanceof UnixSocketAddressWithTransport) {
            UnixSocketAddressWithTransport unixAddr = ((UnixSocketAddressWithTransport) address);

            // TODO: Maybe introduce a `UnixClientChannel` that can handle both stream and datagram sockets? This would
            // Allow us to support `unix://` for both kind of sockets like in go.
            switch (unixAddr.getTransportType()) {
                case UDS_STREAM:
                    return new UnixStreamClientChannel(unixAddr.getAddress(), timeout, connectionTimeout, bufferSize);
                case UDS_DATAGRAM:
                case UDS:
                    return new UnixDatagramClientChannel(unixAddr.getAddress(), timeout, bufferSize);
                default:
                    throw new IllegalArgumentException("Unsupported transport type: " + unixAddr.getTransportType());
            }
        }
        // We keep this for backward compatibility
        try {
            if (Class.forName("jnr.unixsocket.UnixSocketAddress").isInstance(address)) {
                return new UnixDatagramClientChannel(address, timeout, bufferSize);
            }
        } catch (ClassNotFoundException e) {
            // not loaded, can't use
        }

        return new DatagramClientChannel(address);
    }

    abstract class StatsDMessage<T extends Number> extends NumericMessage<T> {
        final double sampleRate; // NaN for none
        final long timestamp; // zero for none

        protected StatsDMessage(String aspect, Message.Type type, T value, double sampleRate, long timestamp,
            TagsCardinality card, String[] tags)
        {
            super(aspect, type, value, card, tags);
            this.sampleRate = sampleRate;
            this.timestamp = timestamp;
        }

        @Override
        public final boolean writeTo(StringBuilder builder, int capacity) {
            builder.append(prefix).append(aspect).append(':');
            writeValue(builder);
            builder.append('|').append(type);
            if (!Double.isNaN(sampleRate)) {
                builder.append('|').append('@').append(format(SAMPLE_RATE_FORMATTER, sampleRate));
            }
            if (timestamp != 0) {
                builder.append("|T").append(timestamp);
            }
            tagString(this.tags, builder);
            writeMessageTail(builder, tagsCardinality);
            return false;
        }

        @Override
        public boolean canAggregate() {
            // Timestamped values can not be aggregated.
            return super.canAggregate() && this.timestamp == 0;
        }

        protected abstract void writeValue(StringBuilder builder);
    }

    void writeMessageTail(StringBuilder builder, TagsCardinality msgTagsCardinality) {
        if (msgTagsCardinality.value != null) {
            builder.append("|card:").append(msgTagsCardinality.value);
        }
        if (containerID != null && !containerID.isEmpty()) {
            builder.append("|c:").append(containerID);
        }
        if (externalEnv != null && !externalEnv.isEmpty()) {
            builder.append("|e:").append(externalEnv);
        }
        builder.append('\n');
    }

    boolean sendMetric(final Message message) {
        return send(message);
    }

    private boolean send(final Message message) {
        boolean success = statsDProcessor.send(message);
        if (success) {
            this.telemetry.incrMetricsSent(1, message.getType());
        } else {
            this.telemetry.incrPacketDroppedQueue(1);
        }

        return success;
    }

    // send double with sample rate and timestamp
    private void send(
        String aspect, final double value, Message.Type type, double sampleRate, long timestamp, TagsCardinality cardinality,
        String[] tags)
    {
        if (statsDProcessor.getAggregator().getFlushInterval() != 0 && !Double.isNaN(sampleRate)) {
            switch (type) {
                case COUNT:
                    sampleRate = Double.NaN;
                    break;
                default:
                    break;
            }
        }

        if (cardinality == null) {
            cardinality = clientTagsCardinality;
        }

        if (Double.isNaN(sampleRate) || !isInvalidSample(sampleRate)) {

            sendMetric(new StatsDMessage<Double>(aspect, type, Double.valueOf(value), sampleRate, timestamp, cardinality, tags) {
                @Override protected void writeValue(StringBuilder builder) {
                    builder.append(format(NUMBER_FORMATTER, this.value));
                }
            });
        }
    }

    private void send(String aspect, final double value, Message.Type type, double sampleRate, final TagsCardinality cardinality,
        String[] tags) {
        send(aspect, value, type, sampleRate, 0, cardinality, tags);
    }

    private void send(String aspect, final double value, Message.Type type, double sampleRate, String[] tags) {
        send(aspect, value, type, sampleRate, 0, clientTagsCardinality, tags);
    }

    // send double without sample rate
    private void send(String aspect, final double value, Message.Type type, String[] tags) {
        send(aspect, value, type, Double.NaN, 0, clientTagsCardinality, tags);
    }

    // send long with sample rate
    private void send(
        String aspect, final long value, Message.Type type, double sampleRate, long timestamp, TagsCardinality cardinality,
        String[] tags)
    {
        if (statsDProcessor.getAggregator().getFlushInterval() != 0 && !Double.isNaN(sampleRate)) {
            switch (type) {
                case COUNT:
                    sampleRate = Double.NaN;
                    break;
                default:
                    break;
            }
        }

        if (cardinality == null) {
            cardinality = clientTagsCardinality;
        }

        if (Double.isNaN(sampleRate) || !isInvalidSample(sampleRate)) {
            sendMetric(new StatsDMessage<Long>(aspect, type, value, sampleRate, timestamp, cardinality, tags) {
                @Override protected void writeValue(StringBuilder builder) {
                    builder.append(this.value.longValue());
                }
            });
        }
    }

    private void send(String aspect, final long value, Message.Type type, double sampleRate, final TagsCardinality cardinality,
        String[] tags) {
        send(aspect, value, type, sampleRate, 0, cardinality, tags);
    }

    private void send(String aspect, final long value, Message.Type type, double sampleRate, String[] tags) {
        send(aspect, value, type, sampleRate, 0, clientTagsCardinality, tags);
    }

    // send long without sample rate
    private void send(String aspect, final long value, Message.Type type, String[] tags) {
        send(aspect, value, type, Double.NaN, 0, clientTagsCardinality, tags);
    }

    private void sendWithTimestamp(String aspect, final double value, Message.Type type, long timestamp, String[] tags) {
        if (timestamp < MIN_TIMESTAMP) {
            timestamp = MIN_TIMESTAMP;
        }
        send(aspect, value, type, Double.NaN, timestamp, clientTagsCardinality, tags);
    }

    private void sendWithTimestamp(String aspect, final double value, Message.Type type, long timestamp,
        final TagsCardinality cardinality, String[] tags) {
        if (timestamp < MIN_TIMESTAMP) {
            timestamp = MIN_TIMESTAMP;
        }
        send(aspect, value, type, Double.NaN, timestamp, cardinality, tags);
    }

    private void sendWithTimestamp(String aspect, final long value, Message.Type type, long timestamp, String[] tags) {
        if (timestamp < MIN_TIMESTAMP) {
            timestamp = MIN_TIMESTAMP;
        }

        send(aspect, value, type, Double.NaN, timestamp, clientTagsCardinality, tags);
    }

    private void sendWithTimestamp(String aspect, final long value, Message.Type type, long timestamp,
        final TagsCardinality cardinality, String[] tags) {
        if (timestamp < MIN_TIMESTAMP) {
            timestamp = MIN_TIMESTAMP;
        }

        send(aspect, value, type, Double.NaN, timestamp, cardinality, tags);
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
        send(aspect, delta, Message.Type.COUNT, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void count(final String aspect, final long delta, final double sampleRate, final String...tags) {
        send(aspect, delta, Message.Type.COUNT, sampleRate, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void count(final String aspect, final long delta, final double sampleRate, final TagsCardinality cardinality,
        final String...tags) {
        send(aspect, delta, Message.Type.COUNT, sampleRate, cardinality, tags);
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
        send(aspect, delta, Message.Type.COUNT, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void count(final String aspect, final double delta, final double sampleRate, final String...tags) {
        send(aspect, delta, Message.Type.COUNT, sampleRate, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void count(final String aspect, final double delta, final double sampleRate, final TagsCardinality cardinality,
        final String...tags) {
        send(aspect, delta, Message.Type.COUNT, sampleRate, cardinality, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void countWithTimestamp(final String aspect, final long value, final long timestamp, final String...tags) {
        sendWithTimestamp(aspect, value, Message.Type.COUNT, timestamp, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void countWithTimestamp(final String aspect, final long value, final long timestamp, final TagsCardinality cardinality,
        final String...tags) {
        sendWithTimestamp(aspect, value, Message.Type.COUNT, timestamp, cardinality, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void countWithTimestamp(final String aspect, final double value, final long timestamp, final String...tags) {
        sendWithTimestamp(aspect, value, Message.Type.COUNT, timestamp, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void countWithTimestamp(final String aspect, final double value, final long timestamp,
        final TagsCardinality cardinality, final String...tags) {
        sendWithTimestamp(aspect, value, Message.Type.COUNT, timestamp, cardinality, tags);
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
        send(aspect, value, Message.Type.GAUGE, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void recordGaugeValue(final String aspect, final double value, final double sampleRate, final String... tags) {
        send(aspect, value, Message.Type.GAUGE, sampleRate, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void recordGaugeValue(final String aspect, final double value, final double sampleRate,
        final TagsCardinality cardinality, final String... tags) {
        send(aspect, value, Message.Type.GAUGE, sampleRate, cardinality, tags);
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
        send(aspect, value, Message.Type.GAUGE, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void recordGaugeValue(final String aspect, final long value, final double sampleRate, final String... tags) {
        send(aspect, value, Message.Type.GAUGE, sampleRate, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void recordGaugeValue(final String aspect, final long value, final double sampleRate,
        final TagsCardinality cardinality, final String... tags) {
        send(aspect, value, Message.Type.GAUGE, sampleRate, cardinality, tags);
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
     * {@inheritDoc}
     */
    @Override
    public void gauge(final String aspect, final double value, final double sampleRate,final TagsCardinality cardinality,
        final String... tags) {
        recordGaugeValue(aspect, value, sampleRate, cardinality, tags);
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
     * {@inheritDoc}
     */
    @Override
    public void gauge(final String aspect, final long value, final double sampleRate, final TagsCardinality cardinality,
        final String... tags) {
        recordGaugeValue(aspect, value, sampleRate, cardinality, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void gaugeWithTimestamp(final String aspect, final double value, final long timestamp, final String... tags) {
        sendWithTimestamp(aspect, value, Message.Type.GAUGE, timestamp, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void gaugeWithTimestamp(final String aspect, final double value, final long timestamp,
        final TagsCardinality cardinality, final String... tags) {
        sendWithTimestamp(aspect, value, Message.Type.GAUGE, timestamp, cardinality, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void gaugeWithTimestamp(final String aspect, final long value, final long timestamp, final String... tags) {
        sendWithTimestamp(aspect, value, Message.Type.GAUGE, timestamp, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void gaugeWithTimestamp(final String aspect, final long value, final long timestamp, final TagsCardinality cardinality,
        final String... tags) {
        sendWithTimestamp(aspect, value, Message.Type.GAUGE, timestamp, cardinality, tags);
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
        send(aspect, timeInMs, Message.Type.TIME, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void recordExecutionTime(final String aspect, final long timeInMs, final double sampleRate, final String... tags) {
        send(aspect, timeInMs, Message.Type.TIME, sampleRate, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void recordExecutionTime(final String aspect, final long timeInMs, final double sampleRate,
        final TagsCardinality cardinality, final String... tags) {
        send(aspect, timeInMs, Message.Type.TIME, sampleRate, cardinality, tags);
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
     * {@inheritDoc}
     */
    @Override
    public void time(final String aspect, final long value, final double sampleRate, final TagsCardinality cardinality,
        final String... tags) {
        recordExecutionTime(aspect, value, sampleRate, cardinality, tags);
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
        send(aspect, value, Message.Type.HISTOGRAM, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void recordHistogramValue(final String aspect, final double value, final double sampleRate, final String... tags) {
        send(aspect, value, Message.Type.HISTOGRAM, sampleRate, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void recordHistogramValue(final String aspect, final double value, final double sampleRate,
        final TagsCardinality cardinality, final String... tags) {
        send(aspect, value, Message.Type.HISTOGRAM, sampleRate, cardinality, tags);
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
        send(aspect, value, Message.Type.HISTOGRAM, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void recordHistogramValue(final String aspect, final long value, final double sampleRate, final String... tags) {
        send(aspect, value, Message.Type.HISTOGRAM, sampleRate, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void recordHistogramValue(final String aspect, final long value, final double sampleRate,
        final TagsCardinality cardinality, final String... tags) {
        send(aspect, value, Message.Type.HISTOGRAM, sampleRate, cardinality, tags);
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
     * {@inheritDoc}
     */
    @Override
    public void histogram(final String aspect, final double value, final double sampleRate, final TagsCardinality cardinality,
        final String... tags) {
        recordHistogramValue(aspect, value, sampleRate, cardinality, tags);
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
     * {@inheritDoc}
     */
    @Override
    public void histogram(final String aspect, final long value, final double sampleRate, final TagsCardinality cardinality,
        final String... tags) {
        recordHistogramValue(aspect, value, sampleRate, cardinality, tags);
    }

    /**
     * Records a value for the specified named distribution.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
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
        send(aspect, value, Message.Type.DISTRIBUTION, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void recordDistributionValue(final String aspect, final double value, final double sampleRate, final String... tags) {
        send(aspect, value, Message.Type.DISTRIBUTION, sampleRate, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void recordDistributionValue(final String aspect, final double value, final double sampleRate,
        final TagsCardinality cardinality, final String... tags) {
        send(aspect, value, Message.Type.DISTRIBUTION, sampleRate, cardinality, tags);
    }

    /**
     * Records a value for the specified named distribution.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
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
        send(aspect, value, Message.Type.DISTRIBUTION, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void recordDistributionValue(final String aspect, final long value, final double sampleRate, final String... tags) {
        send(aspect, value, Message.Type.DISTRIBUTION, sampleRate, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void recordDistributionValue(final String aspect, final long value, final double sampleRate,
        final TagsCardinality cardinality, final String... tags) {
        send(aspect, value, Message.Type.DISTRIBUTION, sampleRate, cardinality, tags);
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
     * {@inheritDoc}
     */
    @Override
    public void distribution(final String aspect, final double value, final double sampleRate, final TagsCardinality cardinality,
        final String... tags) {
        recordDistributionValue(aspect, value, sampleRate, cardinality, tags);
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

    /**
     * {@inheritDoc}
     */
    @Override
    public void distribution(final String aspect, final long value, final double sampleRate, final TagsCardinality cardinality,
        final String... tags) {
        recordDistributionValue(aspect, value, sampleRate, cardinality, tags);
    }

    private StringBuilder eventMap(final Event event, StringBuilder res) {
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

        final String sourceTypeName = event.getSourceTypeName();
        if (sourceTypeName != null) {
            res.append("|s:").append(sourceTypeName);
        }

        return res;
    }

    /**
     * Records an event.
     *
     * <p>This method is a DataDog extension, and may not work with other servers.</p>
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
     *
     * @param event
     *     The event to record
     * @param eventTags
     *     array of tags to be added to the data
     *
     * @see <a href="http://docs.datadoghq.com/guides/dogstatsd/#events-1">
     *     http://docs.datadoghq.com/guides/dogstatsd/#events-1</a>
     */
    @Override
    public void recordEvent(final Event event, final String... eventTags) {
        TagsCardinality cardinality = event.getTagsCardinality();
        if (cardinality == null) {
            cardinality = clientTagsCardinality;
        }
        statsDProcessor.send(new AlphaNumericMessage(Message.Type.EVENT, "", cardinality) {
            @Override public boolean writeTo(StringBuilder builder, int capacity) {
                final String title = escapeEventString(prefix + event.getTitle());
                final String text = escapeEventString(event.getText());
                builder.append(Message.Type.EVENT.toString())
                    .append("{")
                    .append(getUtf8Length(title))
                    .append(",")
                    .append(getUtf8Length(text))
                    .append("}:")
                    .append(title)
                    .append("|");

                if (text != null) {
                    builder.append(text);
                }

                eventMap(event, builder);
                tagString(eventTags, builder);
                writeMessageTail(builder, tagsCardinality);
                return false;
            }
        });
        this.telemetry.incrEventsSent(1);
    }

    private static String escapeEventString(final String title) {
        if (title == null) {
            return null;
        }
        return title.replace("\n", "\\n");
    }

    private int getUtf8Length(final String text) {
        if (text == null) {
            return 0;
        }
        return text.getBytes(UTF_8).length;
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
        TagsCardinality cardinality = sc.getTagsCardinality();
        if (cardinality == null) {
            cardinality = clientTagsCardinality;
        }

        statsDProcessor.send(new AlphaNumericMessage(Message.Type.SERVICE_CHECK, "", cardinality) {
            @Override
            public boolean writeTo(StringBuilder sb, int capacity) {
                // see http://docs.datadoghq.com/guides/dogstatsd/#service-checks
                sb.append(Message.Type.SERVICE_CHECK.toString())
                        .append("|")
                        .append(sc.getName())
                        .append("|")
                        .append(sc.getStatus());
                if (sc.getTimestamp() > 0) {
                    sb.append("|d:").append(sc.getTimestamp());
                }
                if (sc.getHostname() != null) {
                    sb.append("|h:").append(sc.getHostname());
                }
                tagString(sc.getTags(), sb);
                if (sc.getMessage() != null) {
                    sb.append("|m:").append(sc.getEscapedMessage());
                }
                writeMessageTail(sb, tagsCardinality);
                return false;
            }
        });
        this.telemetry.incrServiceChecksSent(1);
    }

    /**
     * Updates and returns tags completed with the entityID tag if needed.
     *
     * @param tags the current constant tags list
     *
     * @param entityID the entityID string provided by argument
     *
     * @return true if tags was modified
     */
    private static boolean updateTagsWithEntityID(final List<String> tags, String entityID) {
        // Support "dd.internal.entity_id" internal tag.
        if (entityID == null || entityID.trim().isEmpty()) {
            // if the entityID parameter is null, default to the environment variable
            entityID = System.getenv(DD_ENTITY_ID_ENV_VAR);
        }
        if (entityID != null && !entityID.trim().isEmpty()) {
            final String entityTag = ENTITY_ID_TAG_NAME + ":" + entityID;
            return tags.add(entityTag);
        }
        return false;
    }

    /**
     * Convenience method equivalent to {@link #recordServiceCheckRun(ServiceCheck sc)}.
     */
    @Override
    public void serviceCheck(final ServiceCheck sc) {
        recordServiceCheckRun(sc);
    }

    @Override
    public void recordSetValue(final String aspect, final String val, final String... tags) {
        recordSetValue(aspect, val, clientTagsCardinality, tags);
    }

    /**
     * Records a value for the specified set.
     *
     * <p>Sets are used to count the number of unique elements in a group. If you want to track the number of
     * unique visitor to your site, sets are a great way to do that.</p>
     *
     * <p>This method is a DataDog extension, and may not work with other servers.</p>
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
     *
     * @param aspect
     *     the name of the set
     * @param val
     *     the value to track
     * @param tags
     *     array of tags to be added to the data
     *
     * @see <a href="http://docs.datadoghq.com/guides/dogstatsd/#sets">http://docs.datadoghq.com/guides/dogstatsd/#sets</a>
     */
    @Override
    public void recordSetValue(final String aspect, final String val, final TagsCardinality cardinality, final String... tags) {
        // documentation is light, but looking at dogstatsd source, we can send string values
        // here instead of numbers
        statsDProcessor.send(new AlphaNumericMessage(aspect, Message.Type.SET, val, cardinality, tags) {
            protected void writeValue(StringBuilder builder) {
                builder.append(getValue());
            }

            @Override
            protected final boolean writeTo(StringBuilder builder, int capacity) {
                builder.append(prefix).append(aspect).append(':');
                writeValue(builder);
                builder.append('|').append(type);
                tagString(this.tags, builder);
                writeMessageTail(builder, tagsCardinality);
                return false;
            }
        });
    }

    protected boolean isInvalidSample(double sampleRate) {
        return sampleRate != 1 && ThreadLocalRandom.current().nextDouble() > sampleRate;
    }

    boolean isOriginDetectionEnabled(boolean originDetectionEnabled) {
        if (!originDetectionEnabled) {
            // origin detection is explicitly disabled
            // or a user-defined container ID was provided
            return false;
        }

        String value = System.getenv(ORIGIN_DETECTION_ENABLED_ENV_VAR);
        value = value != null ? value.trim() : null;
        if (value != null && !value.isEmpty()) {
            return !Arrays.asList("no", "false", "0", "n", "off").contains(value.toLowerCase());
        }

        // DD_ORIGIN_DETECTION_ENABLED is not set or is empty
        // default to true
        return true;
    }

    private String getContainerID(String containerID, boolean originDetectionEnabled) {
        if (containerID != null && !containerID.isEmpty()) {
            return containerID;
        }

        if (isOriginDetectionEnabled(originDetectionEnabled)) {
            CgroupReader reader = new CgroupReader();
            return reader.getContainerID();
        }

        return null;
    }

    private int getPacketSize(ClientChannel chan) {
        return maxPacketSizeBytes > 0 ? maxPacketSizeBytes : chan.getMaxPacketSizeBytes();
    }

    class TelemetryMessage extends NumericMessage<Integer> {
        private final String tagsString;  // pre-baked comma separeated tags string

        protected TelemetryMessage(String metric, Integer value, String tags) {
            super(metric, Message.Type.COUNT, value, clientTagsCardinality, null);
            this.tagsString = tags;
            this.done = true;  // dont aggregate telemetry messages for now
        }

        @Override
        public final boolean writeTo(StringBuilder builder, int capacity) {
            builder.append(aspect)
                .append(':')
                .append(this.value)
                .append('|')
                .append(type)
                .append(tagsString);
            writeMessageTail(builder, tagsCardinality);
            return false;
        }
    }

    public void sendTelemetryMetric(String metric, Integer value) {
        telemetryStatsDProcessor.send(new TelemetryMessage(metric, value, telemetryTags));
    }

    void sendTelemetryMetric(String metric, Integer value, String tags) {
        StringBuilder tagsBuilder = new StringBuilder();
        tagsBuilder.setLength(0);
        tagsBuilder.append(telemetryTags);
        tagsBuilder.append(','); // telemetryTags is never empty
        tagsBuilder.append(tags);
        telemetryStatsDProcessor.send(new TelemetryMessage(metric, value, tagsBuilder.toString()));
    }
}
