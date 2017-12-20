package com.timgroup.statsd;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.charset.Charset;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.util.Formatter;
import java.util.Locale;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;

public abstract class DefaultStatsDClient implements StatsDClient {

    public static final Charset MESSAGE_CHARSET = Charset.forName("UTF-8");
    protected static final int PACKET_SIZE_BYTES = 1400;
    protected static final StatsDClientErrorHandler NO_OP_HANDLER = new StatsDClientErrorHandler() {
        @Override
        public void handle(final Exception e) { /* No-op */ }
    };
    /**
     * Because NumberFormat is not thread-safe we cannot share instances across threads. Use a
     * ThreadLocal to create one pre thread as this seems to offer a significant performance
     * improvement over creating one per-thread: http://stackoverflow.com/a/1285297/2648
     * https://github.com/indeedeng/java-dogstatsd-client/issues/4
     */
    private static final ThreadLocal<NumberFormat> NUMBER_FORMATTERS = new
        ThreadLocal<NumberFormat>() {
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
    private final String prefix;
    private final String constantTagsRendered;

    protected final DatagramChannel clientChannel;
    protected final StatsDClientErrorHandler handler;

    public DefaultStatsDClient(
        final String prefix, String[] constantTags, StatsDClientErrorHandler errorHandler) {
        if ((prefix != null) && (!prefix.isEmpty())) {
            this.prefix = prefix + '.';
        } else {
            this.prefix = "";
        }

        if (errorHandler == null) {
            handler = NO_OP_HANDLER;
        } else {
            handler = errorHandler;
        }

    /* Empty list should be null for faster comparison */
        if ((constantTags != null) && (constantTags.length == 0)) {
            constantTags = null;
        }

        if (constantTags != null) {
            StringBuilder sb = new StringBuilder();
            tagString(constantTags, null, sb);
            constantTagsRendered = sb.toString();
        } else {
            constantTagsRendered = null;
        }

        try {
            clientChannel = DatagramChannel.open();
        } catch (final Exception e) {
            throw new StatsDClientException("Failed to start StatsD client", e);
        }
    }

    /**
     * Send the formatted StatD metric string on the {@link #clientChannel}. This can be done in a
     * background thread, the caller thread or any other threading model the implementation
     * chooses.
     *
     * @param message The StatD-formatted metric string.
     */
    protected abstract void send(String message);

    /**
     * Generate a suffix conveying the given tag list to the client
     */
    private static void tagString(final String[] tags, final String tagPrefix, StringBuilder sb) {
        if (tagPrefix != null) {
            sb.append(tagPrefix);
            if ((tags == null) || (tags.length == 0)) {
                return;
            }
            sb.append(",");
        } else {
            if ((tags == null) || (tags.length == 0)) {
                return;
            }
            sb.append("|#");
        }
        for (int n = tags.length - 1; n >= 0; n--) {
            sb.append(tags[n]);
            if (n > 0) {
                sb.append(",");
            }
        }
    }

    private void tagString(final String[] tags, StringBuilder sb) {
        tagString(tags, constantTagsRendered, sb);
    }

    private StringBuilder createStringBuilder(String aspect) {
        return new StringBuilder(prefix).append(aspect).append(':');
    }

    private void appendSampleRate(double sampleRate, StringBuilder sb) {
        sb.append("|@");
        // Use of a formatter here so that the sample rate stays the same as when using String
      // .format
        // FIXME couldn't the NUMBER_FORMATTERS be used instead?
        new Formatter(sb).format("%f", sampleRate);
    }

    private StringBuilder formatStat(String aspect, long value, String metricType) {
        return createStringBuilder(aspect).append(value).append(metricType);
    }

    private StringBuilder formatStat(String aspect, double value, String metricType) {
        return createStringBuilder(aspect).append(NUMBER_FORMATTERS.get().format(value))
            .append(metricType);
    }

    /**
     * Adjusts the specified counter by a given delta.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
     *
     * @param aspect the name of the counter to adjust
     * @param delta the amount to adjust the counter by
     * @param tags array of tags to be added to the data
     */
    @Override
    public void count(final String aspect, final long delta, final String... tags) {
        StringBuilder sb = formatStat(aspect, delta, "|c");
        send(sb, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void count(final String aspect, final long delta, final double sampleRate,
        final String... tags) {
        if (isInvalidSample(sampleRate)) {
            return;
        }

        StringBuilder sb = formatStat(aspect, delta, "|c");
        send(sb, sampleRate, tags);
    }

    @Override
    public void count(String aspect, double delta, String... tags) {
        StringBuilder sb = formatStat(aspect, delta, "|c");
        send(sb, tags);
    }

    @Override
    public void count(String aspect, double delta, double sampleRate, String... tags) {
        if (isInvalidSample(sampleRate)) {
            return;
        }

        StringBuilder sb = formatStat(aspect, delta, "|c");
        send(sb, sampleRate, tags);
    }

    /**
     * Increments the specified counter by one.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
     *
     * @param aspect the name of the counter to increment
     * @param tags array of tags to be added to the data
     */
    @Override
    public void incrementCounter(final String aspect, final String... tags) {
        count(aspect, 1, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void incrementCounter(final String aspect, final double sampleRate,
        final String... tags) {
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
    public void increment(final String aspect, final double sampleRate, final String... tags) {
        incrementCounter(aspect, sampleRate, tags);
    }

    /**
     * Decrements the specified counter by one.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
     *
     * @param aspect the name of the counter to decrement
     * @param tags array of tags to be added to the data
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
     * @param aspect the name of the gauge
     * @param value the new reading of the gauge
     * @param tags array of tags to be added to the data
     */
    @Override
    public void recordGaugeValue(final String aspect, final double value, final String... tags) {
        StringBuilder sb = formatStat(aspect, value, "|g");
        send(sb, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void recordGaugeValue(final String aspect, final double value, final double sampleRate,
        final String... tags) {
        if (isInvalidSample(sampleRate)) {
            return;
        }
        StringBuilder sb = formatStat(aspect, value, "|g");
        send(sb, sampleRate, tags);
    }

    /**
     * Records the latest fixed value for the specified named gauge.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
     *
     * @param aspect the name of the gauge
     * @param value the new reading of the gauge
     * @param tags array of tags to be added to the data
     */
    @Override
    public void recordGaugeValue(final String aspect, final long value, final String... tags) {
        StringBuilder sb = formatStat(aspect, value, "|g");
        send(sb, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void recordGaugeValue(final String aspect, final long value, final double sampleRate,
        final String... tags) {
        if (isInvalidSample(sampleRate)) {
            return;
        }
        StringBuilder sb = formatStat(aspect, value, "|g");
        send(sb, sampleRate, tags);
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
    public void gauge(final String aspect, final double value, final double sampleRate,
        final String... tags) {
        recordGaugeValue(aspect, value, sampleRate, tags);
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
    public void gauge(final String aspect, final long value, final double sampleRate,
        final String... tags) {
        recordGaugeValue(aspect, value, sampleRate, tags);
    }

    /**
     * Records an execution time in milliseconds for the specified named operation.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
     *
     * @param aspect the name of the timed operation
     * @param timeInMs the time in milliseconds
     * @param tags array of tags to be added to the data
     */
    @Override
    public void recordExecutionTime(final String aspect, final long timeInMs,
        final String... tags) {
        StringBuilder sb = formatStat(aspect, timeInMs, "|ms");
        send(sb, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void recordExecutionTime(final String aspect, final long timeInMs,
        final double sampleRate,
        final String... tags) {
        if (isInvalidSample(sampleRate)) {
            return;
        }
        StringBuilder sb = formatStat(aspect, timeInMs, "|ms");
        send(sb, sampleRate, tags);
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
    public void time(final String aspect, final long value, final double sampleRate,
        final String... tags) {
        recordExecutionTime(aspect, value, sampleRate, tags);
    }

    /**
     * Records a value for the specified named histogram.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
     *
     * @param aspect the name of the histogram
     * @param value the value to be incorporated in the histogram
     * @param tags array of tags to be added to the data
     */
    @Override
    public void recordHistogramValue(final String aspect, final double value,
        final String... tags) {
        StringBuilder sb = formatStat(aspect, value, "|h");
        send(sb, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void recordHistogramValue(final String aspect, final double value,
        final double sampleRate,
        final String... tags) {
        if (isInvalidSample(sampleRate)) {
            return;
        }
        StringBuilder sb = formatStat(aspect, value, "|h");
        send(sb, sampleRate, tags);
    }

    /**
     * Records a value for the specified named histogram.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
     *
     * @param aspect the name of the histogram
     * @param value the value to be incorporated in the histogram
     * @param tags array of tags to be added to the data
     */
    @Override
    public void recordHistogramValue(final String aspect, final long value, final String... tags) {
        StringBuilder sb = formatStat(aspect, value, "|h");
        send(sb, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void recordHistogramValue(final String aspect, final long value, final double sampleRate,
        final String... tags) {
        if (isInvalidSample(sampleRate)) {
            return;
        }
        StringBuilder sb = formatStat(aspect, value, "|h");
        send(sb, sampleRate, tags);
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
    public void histogram(final String aspect, final double value, final double sampleRate,
        final String... tags) {
        recordHistogramValue(aspect, value, sampleRate, tags);
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
    public void histogram(final String aspect, final long value, final double sampleRate,
        final String... tags) {
        recordHistogramValue(aspect, value, sampleRate, tags);
    }

    /**
     * Records a value for the specified named distribution. <p> <p>This method is non-blocking and is guaranteed not to
     * throw an exception.</p>
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
        StringBuilder sb = formatStat(aspect, value, "|d");
        send(sb, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void recordDistributionValue(final String aspect, final double value, final double sampleRate,
        final String... tags) {
        if (isInvalidSample(sampleRate)) {
            return;
        }
        StringBuilder sb = formatStat(aspect, value, "|d");
        send(sb, sampleRate, tags);
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
     * Records a value for the specified named distribution. <p> <p>This method is non-blocking and is guaranteed not to
     * throw an exception.</p>
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
        StringBuilder sb = formatStat(aspect, value, "|d");
        send(sb, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void recordDistributionValue(final String aspect, final long value, final double sampleRate,
        final String... tags) {
        if (isInvalidSample(sampleRate)) {
            return;
        }
        StringBuilder sb = formatStat(aspect, value, "|d");
        send(sb, sampleRate, tags);
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

    private void eventMap(final Event event, StringBuilder sb) {
        final long millisSinceEpoch = event.getMillisSinceEpoch();
        if (millisSinceEpoch != -1) {
            sb.append("|d:").append(millisSinceEpoch / 1000);
        }

        final String hostname = event.getHostname();
        if (hostname != null) {
            sb.append("|h:").append(hostname);
        }

        final String aggregationKey = event.getAggregationKey();
        if (aggregationKey != null) {
            sb.append("|k:").append(aggregationKey);
        }

        final String priority = event.getPriority();
        if (priority != null) {
            sb.append("|p:").append(priority);
        }

        final String alertType = event.getAlertType();
        if (alertType != null) {
            sb.append("|t:").append(alertType);
        }
    }

    /**
     * Records an event
     *
     * <p>This method is a DataDog extension, and may not work with other servers.</p>
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
     *
     * @param event The event to record
     * @param tags array of tags to be added to the data
     * @see <a href="http://docs.datadoghq.com/guides/dogstatsd/#events-1">http://docs.datadoghq
     * .com/guides/dogstatsd/#events-1</a>
     */
    @Override
    public void recordEvent(final Event event, final String... tags) {
        StringBuilder sb = new StringBuilder();
        final String title = escapeEventString(prefix + event.getTitle());
        final String text = escapeEventString(event.getText());
        sb.append("_e{")
            .append(title.length())
            .append(',')
            .append(text.length())
            .append("}:")
            .append(title)
            .append('|')
            .append(text);
        eventMap(event, sb);
        send(sb, tags);
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
     * @param sc the service check object
     */
    @Override
    public void recordServiceCheckRun(final ServiceCheck sc) {
        send(toStatsDString(sc));
    }

    private String toStatsDString(final ServiceCheck sc) {
        // see http://docs.datadoghq.com/guides/dogstatsd/#service-checks
        final StringBuilder sb = new StringBuilder();
        sb.append("_sc|").append(sc.getName()).append('|').append(sc.getStatus());
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
     * Sets are used to count the number of unique elements in a group. If you want to track the
     * number of unique visitor to your site, sets are a great way to do that.
     *
     * <p>This method is a DataDog extension, and may not work with other servers.</p>
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
     *
     * @param aspect the name of the set
     * @param value the value to track
     * @param tags array of tags to be added to the data
     * @see <a href="http://docs.datadoghq.com/guides/dogstatsd/#sets">http://docs.datadoghq
     * .com/guides/dogstatsd/#sets</a>
     */
    @Override
    public void recordSetValue(final String aspect, final String value, final String... tags) {
        // documentation is light, but looking at dogstatsd source, we can send string values
        // here instead of numbers
        StringBuilder sb = createStringBuilder(aspect);
        sb.append(value).append("|s");
        send(sb, tags);
    }

    @Override
    public void close() {
        stop();
    }

    private void send(StringBuilder sb, double sampleRate, String[] tags) {
        appendSampleRate(sampleRate, sb);
        send(sb, tags);
    }

    private void send(StringBuilder sb, String[] tags) {
        tagString(tags, sb);
        send(sb.toString());
    }

    private boolean isInvalidSample(double sampleRate) {
        return sampleRate != 1 && ThreadLocalRandom.current().nextDouble() > sampleRate;
    }

    @Override
    public void stop() {
        if (clientChannel != null) {
            try {
                clientChannel.close();
            } catch (final IOException e) {
                handler.handle(e);
            }
        }
    }

    /**
     * Helper class that is responsible for the IO Operation on the {@link #clientChannel}.
     */
    protected class Sender {

        private final ByteBuffer sendBuffer = ByteBuffer.allocate(PACKET_SIZE_BYTES);
        private final Callable<InetSocketAddress> addressLookup;

        protected Sender(Callable<InetSocketAddress> addressLookup) {
            this.addressLookup = addressLookup;
        }

        /**
         * Add the specified StatsD-formatted String to the IO Buffer, if there are still available
         * space. If the message cannot fit in the buffer, the buffer is sent to the server via
         * {@link #blockingSend()} and then the message gets added to the buffer.
         *
         * @param message The StatsD-formatted String.
         * @throws Exception if the addressLookup fails during the send.
         */
        protected void addToBuffer(String message) throws Exception {
            final byte[] data = message.getBytes(MESSAGE_CHARSET);
            if (sendBuffer.remaining() < (data.length + 1)) {
                blockingSend();
            }
            if (sendBuffer.position() > 0) {
                sendBuffer.put((byte) '\n');
            }
            sendBuffer.put(data);
        }

        /**
         * Send the IO Buffer to the server and ready the buffer to receive more data.
         *
         * @throws Exception if the addressLookup fails.
         */
        protected void blockingSend() throws Exception {
            final InetSocketAddress address = addressLookup.call();
            final int sizeOfBuffer = sendBuffer.position();
            sendBuffer.flip();

            final int sentBytes = clientChannel.send(sendBuffer, address);
            sendBuffer.limit(sendBuffer.capacity());
            sendBuffer.rewind();

            if (sizeOfBuffer != sentBytes) {
                handler.handle(
                    new IOException(
                        String.format(
                            "Could not send entirely stat %s to host %s:%d. Only sent %d bytes "
                                + "out of %d bytes",
                            sendBuffer.toString(),
                            address.getHostName(),
                            address.getPort(),
                            sentBytes,
                            sizeOfBuffer)));
            }
        }
    }

    /**
     * Create dynamic lookup for the given host name and port.
     *
     * @param hostname the host name of the targeted StatsD server
     * @param port the port of the targeted StatsD server
     * @return a function to perform the lookup
     */
    public static Callable<InetSocketAddress> volatileAddressResolution(final String hostname,
        final int port) {
        return new Callable<InetSocketAddress>() {
            @Override
            public InetSocketAddress call() throws UnknownHostException {
                return new InetSocketAddress(InetAddress.getByName(hostname), port);
            }
        };
    }

    /**
     * Lookup the address for the given host name and cache the result.
     *
     * @param hostname the host name of the targeted StatsD server
     * @param port the port of the targeted StatsD server
     * @return a function that cached the result of the lookup
     * @throws Exception if the lookup fails, i.e. {@link UnknownHostException}
     */
    public static Callable<InetSocketAddress> staticAddressResolution(final String hostname, final
    int port) throws Exception {
        final InetSocketAddress address = volatileAddressResolution(hostname, port).call();
        return new Callable<InetSocketAddress>() {
            @Override
            public InetSocketAddress call() {
                return address;
            }
        };
    }

    protected static Callable<InetSocketAddress> staticStatsDAddressResolution(
        final String hostname,
        final int port) throws StatsDClientException {
        try {
            return staticAddressResolution(hostname, port);
        } catch (final Exception e) {
            throw new StatsDClientException("Failed to lookup StatsD host", e);
        }
    }
}
