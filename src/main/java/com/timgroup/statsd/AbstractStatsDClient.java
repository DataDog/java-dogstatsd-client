package com.timgroup.statsd;

import java.nio.charset.Charset;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.util.Locale;

abstract class AbstractStatsDClient implements StatsDClient {

    private static final Charset MESSAGE_CHARSET = Charset.forName("UTF-8");

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

    private final String prefix;
    private final String constantTagsRendered;

    /**
     * @param prefix
     *     the prefix to apply to keys sent via this client
     * @param constantTags
     *     tags to be added to all content sent
     */
    AbstractStatsDClient(final String prefix, String[] constantTags) throws StatsDClientException {
        if((prefix != null) && (!prefix.isEmpty())) {
            this.prefix = String.format("%s.", prefix);
        } else {
            this.prefix = "";
        }

        /* Empty list should be null for faster comparison */
        if((constantTags != null) && (constantTags.length == 0)) {
            constantTags = null;
        }

        if(constantTags != null) {
            constantTagsRendered = tagString(constantTags, null);
        } else {
            constantTagsRendered = null;
        }
    }

    /**
     * Generate a suffix conveying the given tag list to the client
     */
    private static String tagString(final String[] tags, final String tagPrefix) {
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
    private String tagString(final String[] tags) {
        return tagString(tags, constantTagsRendered);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void count(final String aspect, final long delta, final String... tags) {
        send(String.format("%s%s:%d|c%s", prefix, aspect, delta, tagString(tags)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void count(final String aspect, final long delta, final double sampleRate, final String...tags) {
    	if(isInvalidSample(sampleRate)) {
    		return;
    	}
    	send(String.format("%s%s:%d|c|@%f%s", prefix, aspect, delta, sampleRate, tagString(tags)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void incrementCounter(final String aspect, final String... tags) {
        count(aspect, 1, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void incrementCounter(final String aspect, final double sampleRate, final String... tags) {
    	count(aspect, 1, sampleRate, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void increment(final String aspect, final String... tags) {
        incrementCounter(aspect, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void increment(final String aspect, final double sampleRate, final String...tags ) {
    	incrementCounter(aspect, sampleRate, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void decrementCounter(final String aspect, final String... tags) {
        count(aspect, -1, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void decrementCounter(String aspect, final double sampleRate, final String... tags) {
        count(aspect, -1, sampleRate, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void decrement(final String aspect, final String... tags) {
        decrementCounter(aspect, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void decrement(final String aspect, final double sampleRate, final String... tags) {
        decrementCounter(aspect, sampleRate, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void recordGaugeValue(final String aspect, final double value, final String... tags) {
        /* Intentionally using %s rather than %f here to avoid
         * padding with extra 0s to represent precision */
        send(String.format("%s%s:%s|g%s", prefix, aspect, NUMBER_FORMATTERS.get().format(value), tagString(tags)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void recordGaugeValue(final String aspect, final double value, final double sampleRate, final String... tags) {
    	if(isInvalidSample(sampleRate)) {
    		return;
    	}
        send(String.format("%s%s:%s|g|@%f%s", prefix, aspect, NUMBER_FORMATTERS.get().format(value), sampleRate, tagString(tags)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void gauge(final String aspect, final double value, final String... tags) {
        recordGaugeValue(aspect, value, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void gauge(final String aspect, final double value, final double sampleRate, final String... tags) {
        recordGaugeValue(aspect, value, sampleRate, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void recordGaugeValue(final String aspect, final long value, final String... tags) {
        send(String.format("%s%s:%d|g%s", prefix, aspect, value, tagString(tags)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void recordGaugeValue(final String aspect, final long value, final double sampleRate, final String... tags) {
    	if(isInvalidSample(sampleRate)) {
    		return;
    	}
        send(String.format("%s%s:%d|g|@%f%s", prefix, aspect, value, sampleRate, tagString(tags)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void gauge(final String aspect, final long value, final String... tags) {
        recordGaugeValue(aspect, value, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void gauge(final String aspect, final long value, final double sampleRate, final String... tags) {
        recordGaugeValue(aspect, value, sampleRate, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void recordExecutionTime(final String aspect, final long timeInMs, final String... tags) {
        send(String.format("%s%s:%d|ms%s", prefix, aspect, timeInMs, tagString(tags)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void recordExecutionTime(final String aspect, final long timeInMs, final double sampleRate, final String... tags) {
    	if(isInvalidSample(sampleRate)) {
    		return;
    	}
        send(String.format("%s%s:%d|ms|@%f%s", prefix, aspect, timeInMs, sampleRate, tagString(tags)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void time(final String aspect, final long value, final String... tags) {
        recordExecutionTime(aspect, value, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void time(final String aspect, final long value, final double sampleRate, final String... tags) {
        recordExecutionTime(aspect, value, sampleRate, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void recordHistogramValue(final String aspect, final double value, final String... tags) {
        /* Intentionally using %s rather than %f here to avoid
         * padding with extra 0s to represent precision */
        send(String.format("%s%s:%s|h%s", prefix, aspect, NUMBER_FORMATTERS.get().format(value), tagString(tags)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void recordHistogramValue(final String aspect, final double value, final double sampleRate, final String... tags) {
    	if(isInvalidSample(sampleRate)) {
    		return;
    	}
    	  /* Intentionally using %s rather than %f here to avoid
    	   * padding with extra 0s to represent precision */
    	send(String.format("%s%s:%s|h|@%f%s", prefix, aspect, NUMBER_FORMATTERS.get().format(value), sampleRate, tagString(tags)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void histogram(final String aspect, final double value, final String... tags) {
        recordHistogramValue(aspect, value, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void histogram(final String aspect, final double value, final double sampleRate, final String... tags) {
        recordHistogramValue(aspect, value, sampleRate, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void recordHistogramValue(final String aspect, final long value, final String... tags) {
        send(String.format("%s%s:%d|h%s", prefix, aspect, value, tagString(tags)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void recordHistogramValue(final String aspect, final long value, final double sampleRate, final String... tags) {
    	if(isInvalidSample(sampleRate)) {
    		return;
    	}
        send(String.format("%s%s:%d|h|@%f%s", prefix, aspect, value, sampleRate, tagString(tags)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void histogram(final String aspect, final long value, final String... tags) {
        recordHistogramValue(aspect, value, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void histogram(final String aspect, final long value, final double sampleRate, final String... tags) {
        recordHistogramValue(aspect, value, sampleRate, tags);
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
     * {@inheritDoc}
     */
    @Override
    public final void recordEvent(final Event event, final String... tags) {
        final String title = escapeEventString(prefix + event.getTitle());
        final String text = escapeEventString(event.getText());
        send(String.format("_e{%d,%d}:%s|%s%s%s",
                title.length(), text.length(), title, text, eventMap(event), tagString(tags)));
    }

    private String escapeEventString(final String title) {
        return title.replace("\n", "\\n");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void recordServiceCheckRun(final ServiceCheck sc) {
        send(toStatsDString(sc));
    }

    private String toStatsDString(final ServiceCheck sc) {
        // see http://docs.datadoghq.com/guides/dogstatsd/#service-checks
        final StringBuilder sb = new StringBuilder();
        sb.append(String.format("_sc|%s|%d", sc.getName(), sc.getStatus()));
        if (sc.getTimestamp() > 0) {
            sb.append(String.format("|d:%d", sc.getTimestamp()));
        }
        if (sc.getHostname() != null) {
            sb.append(String.format("|h:%s", sc.getHostname()));
        }
        sb.append(tagString(sc.getTags()));
        if (sc.getMessage() != null) {
            sb.append(String.format("|m:%s", sc.getEscapedMessage()));
        }
        return sb.toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void serviceCheck(final ServiceCheck sc) {
        recordServiceCheckRun(sc);
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public final void recordSetValue(final String aspect, final String value, final String... tags) {
        // documentation is light, but looking at dogstatsd source, we can send string values
        // here instead of numbers
        send(String.format("%s%s:%s|s%s", prefix, aspect, value, tagString(tags)));
    }

    private void send(final String message) {
        send(message.getBytes(MESSAGE_CHARSET));
    }

    abstract void send(final byte[] message);

    private boolean isInvalidSample(double sampleRate) {
    	return sampleRate != 1 && Math.random() > sampleRate;
    }

}
