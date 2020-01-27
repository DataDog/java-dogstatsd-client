package com.timgroup.statsd;

/**
 * A No-Op StatsDClient, which can be substituted in when metrics are not
 * required.
 *
 * @author Tom Denley
 *
 */
public final class NoOpStatsDClient implements StatsDClient {
    @Override public void stop() { }

    @Override public void close() { }

    @Override public void count(String aspect, long delta, String... tags) { }

    @Override public void count(String aspect, long delta, double sampleRate, String... tags) { }

    @Override public void count(String aspect, double delta, String... tags) { }

    @Override public void count(String aspect, double delta, double sampleRate, String... tags) { }

    @Override public void incrementCounter(String aspect, String... tags) { }

    @Override public void incrementCounter(String aspect, double sampleRate, String... tags) { }

    @Override public void increment(String aspect, String... tags) { }

    @Override public void increment(String aspect, double sampleRate, String...tags) { }

    @Override public void decrementCounter(String aspect, String... tags) { }

    @Override public void decrementCounter(String aspect, double sampleRate, String... tags) { }

    @Override public void decrement(String aspect, String... tags) { }

    @Override public void decrement(String aspect, double sampleRate, String... tags) { }

    @Override public void recordGaugeValue(String aspect, double value, String... tags) { }

    @Override public void recordGaugeValue(String aspect, double value, double sampleRate, String... tags) { }

    @Override public void recordGaugeValue(String aspect, long value, String... tags) { }

    @Override public void recordGaugeValue(String aspect, long value, double sampleRate, String... tags) { }

    @Override public void gauge(String aspect, double value, String... tags) { }

    @Override public void gauge(String aspect, double value, double sampleRate, String... tags) { }

    @Override public void gauge(String aspect, long value, String... tags) { }

    @Override public void gauge(String aspect, long value, double sampleRate, String... tags) { }

    @Override public void recordExecutionTime(String aspect, long timeInMs, String... tags) { }

    @Override public void recordExecutionTime(String aspect, long timeInMs, double sampleRate, String... tags) { }

    @Override public void time(String aspect, long value, String... tags) { }

    @Override public void time(String aspect, long value, double sampleRate, String... tags) { }

    @Override public void recordHistogramValue(String aspect, double value, String... tags) { }

    @Override public void recordHistogramValue(String aspect, double value, double sampleRate, String... tags) { }

    @Override public void recordHistogramValue(String aspect, long value, String... tags) { }

    @Override public void recordHistogramValue(String aspect, long value, double sampleRate, String... tags) { }

    @Override public void histogram(String aspect, double value, String... tags) { }

    @Override public void histogram(String aspect, double value, double sampleRate, String... tags) { }

    @Override public void histogram(String aspect, long value, String... tags) { }

    @Override public void histogram(String aspect, long value, double sampleRate, String... tags) { }

    @Override public void recordDistributionValue(String aspect, double value, String... tags) { }

    @Override public void recordDistributionValue(String aspect, double value, double sampleRate, String... tags) { }

    @Override public void recordDistributionValue(String aspect, long value, String... tags) { }

    @Override public void recordDistributionValue(String aspect, long value, double sampleRate, String... tags) { }

    @Override public void distribution(String aspect, double value, String... tags) { }

    @Override public void distribution(String aspect, double value, double sampleRate, String... tags) { }

    @Override public void distribution(String aspect, long value, String... tags) { }

    @Override public void distribution(String aspect, long value, double sampleRate, String... tags) { }

    @Override public void recordEvent(final Event event, final String... tags) { }

    @Override public void recordServiceCheckRun(ServiceCheck sc) { }

    @Override public void serviceCheck(ServiceCheck sc) { }

    @Override public void recordSetValue(String aspect, String value, String... tags) { }
}
