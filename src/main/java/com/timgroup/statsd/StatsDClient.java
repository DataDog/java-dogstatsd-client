package com.timgroup.statsd;

import java.io.Closeable;

/**
 * Describes a client connection to a StatsD server, which may be used to post metrics in the form
 * of counters, timers, and gauges.
 *
 * <p>Three key methods are provided for the submission of data-points for the application under
 * scrutiny:
 *
 * <ul>
 *   <li>{@link #incrementCounter} - adds one to the value of the specified named counter
 *   <li>{@link #recordGaugeValue} - records the latest fixed value for the specified named gauge
 *   <li>{@link #recordExecutionTime} - records an execution time in milliseconds for the specified
 *       named operation
 * </ul>
 *
 * @author Tom Denley
 */
public interface StatsDClient extends Closeable {

    /**
     * Cleanly shut down this StatsD client. This method may throw an exception if the socket cannot
     * be closed.
     */
    void stop();

    /**
     * Stop the statsd client.
     *
     * @see #stop()
     */
    @Override
    void close();

    /**
     * Adjusts the specified counter by a given delta.
     *
     * <p>This method is a DataDog extension, and may not work with other servers.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.
     *
     * @param aspect the name of the counter to adjust
     * @param delta the amount to adjust the counter by
     * @param tags array of tags to be added to the data
     */
    void count(String aspect, long delta, String... tags);

    /**
     * Adjusts the specified counter by a given delta.
     *
     * <p>This method is a DataDog extension, and may not work with other servers.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.
     *
     * @param aspect the name of the counter to adjust
     * @param delta the amount to adjust the counter by
     * @param sampleRate percentage of time metric to be sent
     * @param tags array of tags to be added to the data
     */
    void count(String aspect, long delta, double sampleRate, String... tags);

    /**
     * Adjusts the specified counter by a given delta.
     *
     * <p>This method is a DataDog extension, and may not work with other servers.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.
     *
     * @param aspect the name of the counter to adjust
     * @param delta the amount to adjust the counter by
     * @param sampleRate percentage of time metric to be sent
     * @param cardinality specify cardinality for Kubernetes or Docker tags added by the Agent
     * @param tags array of tags to be added to the data
     */
    void count(
            String aspect,
            long delta,
            double sampleRate,
            TagsCardinality cardinality,
            String... tags);

    /**
     * Adjusts the specified counter by a given delta.
     *
     * <p>This method is a DataDog extension, and may not work with other servers.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.
     *
     * @param aspect the name of the counter to adjust
     * @param delta the amount to adjust the counter by
     * @param tags array of tags to be added to the data
     */
    void count(String aspect, double delta, String... tags);

    /**
     * Adjusts the specified counter by a given delta.
     *
     * <p>This method is a DataDog extension, and may not work with other servers.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.
     *
     * @param aspect the name of the counter to adjust
     * @param delta the amount to adjust the counter by
     * @param sampleRate percentage of time metric to be sent
     * @param tags array of tags to be added to the data
     */
    void count(String aspect, double delta, double sampleRate, String... tags);

    /**
     * Adjusts the specified counter by a given delta.
     *
     * <p>This method is a DataDog extension, and may not work with other servers.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.
     *
     * @param aspect the name of the counter to adjust
     * @param delta the amount to adjust the counter by
     * @param sampleRate percentage of time metric to be sent
     * @param cardinality specify cardinality for Kubernetes or Docker tags added by the Agent
     * @param tags array of tags to be added to the data
     */
    void count(
            String aspect,
            double delta,
            double sampleRate,
            TagsCardinality cardinality,
            String... tags);

    /**
     * Set the counter metric at the given time to the specified value.
     *
     * <p>Values with an explicit timestamp are never aggregated and will be recorded as the metric
     * value at the given time.
     *
     * <p>This method is a DataDog extension, and may not work with other servers.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.
     *
     * @param aspect the name of the counter to adjust
     * @param value the amount to adjust the counter by
     * @param timestamp timestamp of the value, as seconds from the epoch of 1970-01-01T00:00:00Z
     * @param tags array of tags to be added to the data
     */
    void countWithTimestamp(String aspect, long value, long timestamp, String... tags);

    /**
     * Set the counter metric at the given time to the specified value.
     *
     * <p>Values with an explicit timestamp are never aggregated and will be recorded as the metric
     * value at the given time.
     *
     * <p>This method is a DataDog extension, and may not work with other servers.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.
     *
     * @param aspect the name of the counter to adjust
     * @param value the amount to adjust the counter by
     * @param timestamp timestamp of the value, as seconds from the epoch of 1970-01-01T00:00:00Z
     * @param cardinality specify cardinality for Kubernetes or Docker tags added by the Agent
     * @param tags array of tags to be added to the data
     */
    void countWithTimestamp(
            String aspect, long value, long timestamp, TagsCardinality cardinality, String... tags);

    /**
     * Set the counter metric at the given time to the specified value.
     *
     * <p>Values with an explicit timestamp are never aggregated and will be recorded as the metric
     * value at the given time.
     *
     * <p>This method is a DataDog extension, and may not work with other servers.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.
     *
     * @param aspect the name of the counter to adjust
     * @param value the amount to adjust the counter by
     * @param timestamp timestamp of the value, as seconds from the epoch of 1970-01-01T00:00:00Z
     * @param tags array of tags to be added to the data
     */
    void countWithTimestamp(String aspect, double value, long timestamp, String... tags);

    /**
     * Set the counter metric at the given time to the specified value.
     *
     * <p>Values with an explicit timestamp are never aggregated and will be recorded as the metric
     * value at the given time.
     *
     * <p>This method is a DataDog extension, and may not work with other servers.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.
     *
     * @param aspect the name of the counter to adjust
     * @param value the amount to adjust the counter by
     * @param timestamp timestamp of the value, as seconds from the epoch of 1970-01-01T00:00:00Z
     * @param cardinality specify cardinality for Kubernetes or Docker tags added by the Agent
     * @param tags array of tags to be added to the data
     */
    void countWithTimestamp(
            String aspect,
            double value,
            long timestamp,
            TagsCardinality cardinality,
            String... tags);

    /**
     * Increments the specified counter by one.
     *
     * <p>This method is a DataDog extension, and may not work with other servers.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.
     *
     * @param aspect the name of the counter to increment
     * @param tags array of tags to be added to the data
     */
    void incrementCounter(String aspect, String... tags);

    /**
     * Increments the specified counter by one.
     *
     * <p>This method is a DataDog extension, and may not work with other servers.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.
     *
     * @param aspect the name of the counter to increment
     * @param sampleRate percentage of time metric to be sent
     * @param tags array of tags to be added to the data
     */
    void incrementCounter(String aspect, double sampleRate, String... tags);

    /**
     * Convenience method equivalent to {@link #incrementCounter(String, String[])}.
     *
     * @param aspect the name of the counter to increment
     * @param tags array of tags to be added to the data
     */
    void increment(String aspect, String... tags);

    /**
     * Convenience method equivalent to {@link #incrementCounter(String, double, String[])}.
     *
     * @param aspect the name of the counter to increment
     * @param sampleRate percentage of time metric to be sent
     * @param tags array of tags to be added to the data
     */
    void increment(String aspect, double sampleRate, String... tags);

    /**
     * Decrements the specified counter by one.
     *
     * <p>This method is a DataDog extension, and may not work with other servers.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.
     *
     * @param aspect the name of the counter to decrement
     * @param tags array of tags to be added to the data
     */
    void decrementCounter(String aspect, String... tags);

    /**
     * Decrements the specified counter by one.
     *
     * <p>This method is a DataDog extension, and may not work with other servers.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.
     *
     * @param aspect the name of the counter to decrement
     * @param sampleRate percentage of time metric to be sent
     * @param tags array of tags to be added to the data
     */
    void decrementCounter(String aspect, double sampleRate, String... tags);

    /**
     * Convenience method equivalent to {@link #decrementCounter(String, String[])}.
     *
     * @param aspect the name of the counter to decrement
     * @param tags array of tags to be added to the data
     */
    void decrement(String aspect, String... tags);

    /**
     * Convenience method equivalent to {@link #decrementCounter(String, double, String[])}.
     *
     * @param aspect the name of the counter to decrement
     * @param sampleRate percentage of time metric to be sent
     * @param tags array of tags to be added to the data
     */
    void decrement(String aspect, double sampleRate, String... tags);

    /**
     * Records the latest fixed value for the specified named gauge.
     *
     * <p>This method is a DataDog extension, and may not work with other servers.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.
     *
     * @param aspect the name of the gauge
     * @param value the new reading of the gauge
     * @param tags array of tags to be added to the data
     */
    void recordGaugeValue(String aspect, double value, String... tags);

    /**
     * Records the latest fixed value for the specified named gauge.
     *
     * <p>This method is a DataDog extension, and may not work with other servers.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.
     *
     * @param aspect the name of the gauge
     * @param value the new reading of the gauge
     * @param sampleRate percentage of time metric to be sent
     * @param tags array of tags to be added to the data
     */
    void recordGaugeValue(String aspect, double value, double sampleRate, String... tags);

    /**
     * Records the latest fixed value for the specified named gauge.
     *
     * <p>This method is a DataDog extension, and may not work with other servers.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.
     *
     * @param aspect the name of the gauge
     * @param value the new reading of the gauge
     * @param sampleRate percentage of time metric to be sent
     * @param cardinality specify cardinality for Kubernetes or Docker tags added by the Agent
     * @param tags array of tags to be added to the data
     */
    void recordGaugeValue(
            String aspect,
            double value,
            double sampleRate,
            TagsCardinality cardinality,
            String... tags);

    /**
     * Records the latest fixed value for the specified named gauge.
     *
     * <p>This method is a DataDog extension, and may not work with other servers.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.
     *
     * @param aspect the name of the gauge
     * @param value the new reading of the gauge
     * @param tags array of tags to be added to the data
     */
    void recordGaugeValue(String aspect, long value, String... tags);

    /**
     * Records the latest fixed value for the specified named gauge.
     *
     * <p>This method is a DataDog extension, and may not work with other servers.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.
     *
     * @param aspect the name of the gauge
     * @param value the new reading of the gauge
     * @param sampleRate percentage of time metric to be sent
     * @param tags array of tags to be added to the data
     */
    void recordGaugeValue(String aspect, long value, double sampleRate, String... tags);

    /**
     * Records the latest fixed value for the specified named gauge.
     *
     * <p>This method is a DataDog extension, and may not work with other servers.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.
     *
     * @param aspect the name of the gauge
     * @param value the new reading of the gauge
     * @param sampleRate percentage of time metric to be sent
     * @param cardinality specify cardinality for Kubernetes or Docker tags added by the Agent
     * @param tags array of tags to be added to the data
     */
    void recordGaugeValue(
            String aspect,
            long value,
            double sampleRate,
            TagsCardinality cardinality,
            String... tags);

    /**
     * Convenience method equivalent to {@link #recordGaugeValue(String, double, String[])}.
     *
     * @param aspect the name of the gauge
     * @param value the new reading of the gauge
     * @param tags array of tags to be added to the data
     */
    void gauge(String aspect, double value, String... tags);

    /**
     * Convenience method equivalent to {@link #recordGaugeValue(String, double, double, String[])}.
     *
     * @param aspect the name of the gauge
     * @param value the new reading of the gauge
     * @param sampleRate percentage of time metric to be sent
     * @param tags array of tags to be added to the data
     */
    void gauge(String aspect, double value, double sampleRate, String... tags);

    /**
     * Convenience method equivalent to {@link #recordGaugeValue(String, double, double,
     * TagsCardinality, String[])}.
     *
     * @param aspect the name of the gauge
     * @param value the new reading of the gauge
     * @param sampleRate percentage of time metric to be sent
     * @param cardinality specify cardinality for Kubernetes or Docker tags added by the Agent
     * @param tags array of tags to be added to the data
     */
    void gauge(
            String aspect,
            double value,
            double sampleRate,
            TagsCardinality cardinality,
            String... tags);

    /**
     * Convenience method equivalent to {@link #recordGaugeValue(String, long, String[])}.
     *
     * @param aspect the name of the gauge
     * @param value the new reading of the gauge
     * @param tags array of tags to be added to the data
     */
    void gauge(String aspect, long value, String... tags);

    /**
     * Convenience method equivalent to {@link #recordGaugeValue(String, long, double, String[])}.
     *
     * @param aspect the name of the gauge
     * @param value the new reading of the gauge
     * @param sampleRate percentage of time metric to be sent
     * @param tags array of tags to be added to the data
     */
    void gauge(String aspect, long value, double sampleRate, String... tags);

    /**
     * Convenience method equivalent to {@link #recordGaugeValue(String, long, double,
     * TagsCardinality, String[])}.
     *
     * @param aspect the name of the gauge
     * @param value the new reading of the gauge
     * @param sampleRate percentage of time metric to be sent
     * @param cardinality specify cardinality for Kubernetes or Docker tags added by the Agent
     * @param tags array of tags to be added to the data
     */
    void gauge(
            String aspect,
            long value,
            double sampleRate,
            TagsCardinality cardinality,
            String... tags);

    /**
     * Set the gauge metric at the given time to the specified value.
     *
     * <p>Values with an explicit timestamp are never aggregated and will be recorded as the metric
     * value at the given time.
     *
     * @param aspect the name of the gauge
     * @param value the new reading of the gauge
     * @param timestamp timestamp of the value, as seconds from the epoch of 1970-01-01T00:00:00Z
     * @param tags array of tags to be added to the data
     */
    void gaugeWithTimestamp(String aspect, double value, long timestamp, String... tags);

    /**
     * Set the gauge metric at the given time to the specified value.
     *
     * <p>Values with an explicit timestamp are never aggregated and will be recorded as the metric
     * value at the given time.
     *
     * @param aspect the name of the gauge
     * @param value the new reading of the gauge
     * @param timestamp timestamp of the value, as seconds from the epoch of 1970-01-01T00:00:00Z
     * @param cardinality specify cardinality for Kubernetes or Docker tags added by the Agent
     * @param tags array of tags to be added to the data
     */
    void gaugeWithTimestamp(
            String aspect,
            double value,
            long timestamp,
            TagsCardinality cardinality,
            String... tags);

    /**
     * Set the gauge metric at the given time to the specified value.
     *
     * <p>Values with an explicit timestamp are never aggregated and will be recorded as the metric
     * value at the given time.
     *
     * @param aspect the name of the gauge
     * @param value the new reading of the gauge
     * @param timestamp timestamp of the value, as seconds from the epoch of 1970-01-01T00:00:00Z
     * @param tags array of tags to be added to the data
     */
    void gaugeWithTimestamp(String aspect, long value, long timestamp, String... tags);

    /**
     * Set the gauge metric at the given time to the specified value.
     *
     * <p>Values with an explicit timestamp are never aggregated and will be recorded as the metric
     * value at the given time.
     *
     * @param aspect the name of the gauge
     * @param value the new reading of the gauge
     * @param timestamp timestamp of the value, as seconds from the epoch of 1970-01-01T00:00:00Z
     * @param cardinality specify cardinality for Kubernetes or Docker tags added by the Agent
     * @param tags array of tags to be added to the data
     */
    void gaugeWithTimestamp(
            String aspect, long value, long timestamp, TagsCardinality cardinality, String... tags);

    /**
     * Records an execution time in milliseconds for the specified named operation.
     *
     * <p>This method is a DataDog extension, and may not work with other servers.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.
     *
     * @param aspect the name of the timed operation
     * @param timeInMs the time in milliseconds
     * @param tags array of tags to be added to the data
     */
    void recordExecutionTime(String aspect, long timeInMs, String... tags);

    /**
     * Records an execution time in milliseconds for the specified named operation.
     *
     * <p>This method is a DataDog extension, and may not work with other servers.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.
     *
     * @param aspect the name of the timed operation
     * @param timeInMs the time in milliseconds
     * @param sampleRate percentage of time metric to be sent
     * @param tags array of tags to be added to the data
     */
    void recordExecutionTime(String aspect, long timeInMs, double sampleRate, String... tags);

    /**
     * Records an execution time in milliseconds for the specified named operation.
     *
     * <p>This method is a DataDog extension, and may not work with other servers.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.
     *
     * @param aspect the name of the timed operation
     * @param timeInMs the time in milliseconds
     * @param sampleRate percentage of time metric to be sent
     * @param cardinality specify cardinality for Kubernetes or Docker tags added by the Agent
     * @param tags array of tags to be added to the data
     */
    void recordExecutionTime(
            String aspect,
            long timeInMs,
            double sampleRate,
            TagsCardinality cardinality,
            String... tags);

    /**
     * Convenience method equivalent to {@link #recordExecutionTime(String, long, String[])}.
     *
     * @param aspect the name of the timed operation
     * @param value the time in milliseconds
     * @param tags array of tags to be added to the data
     */
    void time(String aspect, long value, String... tags);

    /**
     * Convenience method equivalent to {@link #recordExecutionTime(String, long, double,
     * String[])}.
     *
     * @param aspect the name of the timed operation
     * @param value the time in milliseconds
     * @param sampleRate percentage of time metric to be sent
     * @param tags array of tags to be added to the data
     */
    void time(String aspect, long value, double sampleRate, String... tags);

    /**
     * Convenience method equivalent to {@link #recordExecutionTime(String, long, double,
     * TagsCardinality, String[])}.
     *
     * @param aspect the name of the timed operation
     * @param value the time in milliseconds
     * @param sampleRate percentage of time metric to be sent
     * @param cardinality specify cardinality for Kubernetes or Docker tags added by the Agent
     * @param tags array of tags to be added to the data
     */
    void time(
            String aspect,
            long value,
            double sampleRate,
            TagsCardinality cardinality,
            String... tags);

    /**
     * Records a value for the specified named histogram.
     *
     * <p>This method is a DataDog extension, and may not work with other servers.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.
     *
     * @param aspect the name of the histogram
     * @param value the value to be incorporated in the histogram
     * @param tags array of tags to be added to the data
     */
    void recordHistogramValue(String aspect, double value, String... tags);

    /**
     * Records a value for the specified named histogram.
     *
     * <p>This method is a DataDog extension, and may not work with other servers.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.
     *
     * @param aspect the name of the histogram
     * @param value the value to be incorporated in the histogram
     * @param sampleRate percentage of time metric to be sent
     * @param tags array of tags to be added to the data
     */
    void recordHistogramValue(String aspect, double value, double sampleRate, String... tags);

    /**
     * Records a value for the specified named histogram.
     *
     * <p>This method is a DataDog extension, and may not work with other servers.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.
     *
     * @param aspect the name of the histogram
     * @param value the value to be incorporated in the histogram
     * @param sampleRate percentage of time metric to be sent
     * @param cardinality specify cardinality for Kubernetes or Docker tags added by the Agent
     * @param tags array of tags to be added to the data
     */
    void recordHistogramValue(
            String aspect,
            double value,
            double sampleRate,
            TagsCardinality cardinality,
            String... tags);

    /**
     * Records a value for the specified named histogram.
     *
     * <p>This method is a DataDog extension, and may not work with other servers.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.
     *
     * @param aspect the name of the histogram
     * @param value the value to be incorporated in the histogram
     * @param tags array of tags to be added to the data
     */
    void recordHistogramValue(String aspect, long value, String... tags);

    /**
     * Records a value for the specified named histogram.
     *
     * <p>This method is a DataDog extension, and may not work with other servers.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.
     *
     * @param aspect the name of the histogram
     * @param value the value to be incorporated in the histogram
     * @param sampleRate percentage of time metric to be sent
     * @param tags array of tags to be added to the data
     */
    void recordHistogramValue(String aspect, long value, double sampleRate, String... tags);

    /**
     * Records a value for the specified named histogram.
     *
     * <p>This method is a DataDog extension, and may not work with other servers.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.
     *
     * @param aspect the name of the histogram
     * @param value the value to be incorporated in the histogram
     * @param sampleRate percentage of time metric to be sent
     * @param cardinality specify cardinality for Kubernetes or Docker tags added by the Agent
     * @param tags array of tags to be added to the data
     */
    void recordHistogramValue(
            String aspect,
            long value,
            double sampleRate,
            TagsCardinality cardinality,
            String... tags);

    /**
     * Convenience method equivalent to {@link #recordHistogramValue(String, double, String[])}.
     *
     * @param aspect the name of the histogram
     * @param value the value to be incorporated in the histogram
     * @param tags array of tags to be added to the data
     */
    void histogram(String aspect, double value, String... tags);

    /**
     * Convenience method equivalent to {@link #recordHistogramValue(String, double, double,
     * String[])}.
     *
     * @param aspect the name of the histogram
     * @param value the value to be incorporated in the histogram
     * @param sampleRate percentage of time metric to be sent
     * @param tags array of tags to be added to the data
     */
    void histogram(String aspect, double value, double sampleRate, String... tags);

    /**
     * Convenience method equivalent to {@link #recordHistogramValue(String, double, double,
     * TagsCardinality, String[])}.
     *
     * @param aspect the name of the histogram
     * @param value the value to be incorporated in the histogram
     * @param sampleRate percentage of time metric to be sent
     * @param cardinality specify cardinality for Kubernetes or Docker tags added by the Agent
     * @param tags array of tags to be added to the data
     */
    void histogram(
            String aspect,
            double value,
            double sampleRate,
            TagsCardinality cardinality,
            String... tags);

    /**
     * Convenience method equivalent to {@link #recordHistogramValue(String, long, String[])}.
     *
     * @param aspect the name of the histogram
     * @param value the value to be incorporated in the histogram
     * @param tags array of tags to be added to the data
     */
    void histogram(String aspect, long value, String... tags);

    /**
     * Convenience method equivalent to {@link #recordHistogramValue(String, long, double,
     * String[])}.
     *
     * @param aspect the name of the histogram
     * @param value the value to be incorporated in the histogram
     * @param sampleRate percentage of time metric to be sent
     * @param tags array of tags to be added to the data
     */
    void histogram(String aspect, long value, double sampleRate, String... tags);

    /**
     * Convenience method equivalent to {@link #recordHistogramValue(String, long, double,
     * TagsCardinality, String[])}.
     *
     * @param aspect the name of the histogram
     * @param value the value to be incorporated in the histogram
     * @param sampleRate percentage of time metric to be sent
     * @param cardinality specify cardinality for Kubernetes or Docker tags added by the Agent
     * @param tags array of tags to be added to the data
     */
    void histogram(
            String aspect,
            long value,
            double sampleRate,
            TagsCardinality cardinality,
            String... tags);

    /**
     * Records a value for the specified named distribution.
     *
     * <p>This method is a DataDog extension, and may not work with other servers.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.
     *
     * @param aspect the name of the distribution
     * @param value the value to be incorporated in the distribution
     * @param tags array of tags to be added to the data
     */
    void recordDistributionValue(String aspect, double value, String... tags);

    /**
     * Records a value for the specified named distribution.
     *
     * <p>This method is a DataDog extension, and may not work with other servers.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.
     *
     * @param aspect the name of the distribution
     * @param value the value to be incorporated in the distribution
     * @param sampleRate percentage of time metric to be sent
     * @param tags array of tags to be added to the data
     */
    void recordDistributionValue(String aspect, double value, double sampleRate, String... tags);

    /**
     * Records a value for the specified named distribution.
     *
     * <p>This method is a DataDog extension, and may not work with other servers.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.
     *
     * @param aspect the name of the distribution
     * @param value the value to be incorporated in the distribution
     * @param sampleRate percentage of time metric to be sent
     * @param cardinality specify cardinality for Kubernetes or Docker tags added by the Agent
     * @param tags array of tags to be added to the data
     */
    void recordDistributionValue(
            String aspect,
            double value,
            double sampleRate,
            TagsCardinality cardinality,
            String... tags);

    /**
     * Records a value for the specified named distribution.
     *
     * <p>This method is a DataDog extension, and may not work with other servers.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.
     *
     * @param aspect the name of the distribution
     * @param value the value to be incorporated in the distribution
     * @param tags array of tags to be added to the data
     */
    void recordDistributionValue(String aspect, long value, String... tags);

    /**
     * Records a value for the specified named distribution.
     *
     * <p>This method is a DataDog extension, and may not work with other servers.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.
     *
     * @param aspect the name of the distribution
     * @param value the value to be incorporated in the distribution
     * @param sampleRate percentage of time metric to be sent
     * @param tags array of tags to be added to the data
     */
    void recordDistributionValue(String aspect, long value, double sampleRate, String... tags);

    /**
     * Records a value for the specified named distribution.
     *
     * <p>This method is a DataDog extension, and may not work with other servers.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.
     *
     * @param aspect the name of the distribution
     * @param value the value to be incorporated in the distribution
     * @param sampleRate percentage of time metric to be sent
     * @param cardinality specify cardinality for Kubernetes or Docker tags added by the Agent
     * @param tags array of tags to be added to the data
     */
    void recordDistributionValue(
            String aspect,
            long value,
            double sampleRate,
            TagsCardinality cardinality,
            String... tags);

    /**
     * Convenience method equivalent to {@link #recordDistributionValue(String, double, String[])}.
     *
     * @param aspect the name of the distribution
     * @param value the value to be incorporated in the distribution
     * @param tags array of tags to be added to the data
     */
    void distribution(String aspect, double value, String... tags);

    /**
     * Convenience method equivalent to {@link #recordDistributionValue(String, double, double,
     * String[])}.
     *
     * @param aspect the name of the distribution
     * @param value the value to be incorporated in the distribution
     * @param sampleRate percentage of time metric to be sent
     * @param tags array of tags to be added to the data
     */
    void distribution(String aspect, double value, double sampleRate, String... tags);

    /**
     * Convenience method equivalent to {@link #recordDistributionValue(String, double, double,
     * TagsCardinality, String[])}.
     *
     * @param aspect the name of the distribution
     * @param value the value to be incorporated in the distribution
     * @param sampleRate percentage of time metric to be sent
     * @param tags array of tags to be added to the data
     */
    void distribution(
            String aspect,
            double value,
            double sampleRate,
            TagsCardinality cardinality,
            String... tags);

    /**
     * Convenience method equivalent to {@link #recordDistributionValue(String, long, String[])}.
     *
     * @param aspect the name of the distribution
     * @param value the value to be incorporated in the distribution
     * @param tags array of tags to be added to the data
     */
    void distribution(String aspect, long value, String... tags);

    /**
     * Convenience method equivalent to {@link #recordDistributionValue(String, long, double,
     * String[])}.
     *
     * @param aspect the name of the distribution
     * @param value the value to be incorporated in the distribution
     * @param sampleRate percentage of time metric to be sent
     * @param tags array of tags to be added to the data
     */
    void distribution(String aspect, long value, double sampleRate, String... tags);

    /**
     * Convenience method equivalent to {@link #recordDistributionValue(String, long, double,
     * TagsCardinality, String[])}.
     *
     * @param aspect the name of the distribution
     * @param value the value to be incorporated in the distribution
     * @param sampleRate percentage of time metric to be sent
     * @param cardinality specify cardinality for Kubernetes or Docker tags added by the Agent
     * @param tags array of tags to be added to the data
     */
    void distribution(
            String aspect,
            long value,
            double sampleRate,
            TagsCardinality cardinality,
            String... tags);

    /**
     * Records an event.
     *
     * <p>This method is a DataDog extension, and may not work with other servers.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.
     *
     * @param event The event to record
     * @param tags array of tags to be added to the data
     * @see <a href="http://docs.datadoghq.com/guides/dogstatsd/#events-1">
     *     http://docs.datadoghq.com/guides/dogstatsd/#events-1</a>
     */
    void recordEvent(Event event, String... tags);

    /**
     * Records a run status for the specified named service check.
     *
     * @param sc the service check object
     */
    void recordServiceCheckRun(ServiceCheck sc);

    /**
     * Convenience method equivalent to {@link #recordServiceCheckRun(ServiceCheck sc)}.
     *
     * @param sc the service check object
     */
    void serviceCheck(ServiceCheck sc);

    /**
     * Records a value for the specified set.
     *
     * <p>Sets are used to count the number of unique elements in a group. If you want to track the
     * number of unique visitor to your site, sets are a great way to do that.
     *
     * <p>This method is a DataDog extension, and may not work with other servers.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.
     *
     * @param aspect the name of the set
     * @param value the value to track
     * @param tags array of tags to be added to the data
     * @see <a
     *     href="http://docs.datadoghq.com/guides/dogstatsd/#sets">http://docs.datadoghq.com/guides/dogstatsd/#sets</a>
     */
    void recordSetValue(String aspect, String value, String... tags);

    /**
     * Records a value for the specified set.
     *
     * <p>Sets are used to count the number of unique elements in a group. If you want to track the
     * number of unique visitor to your site, sets are a great way to do that.
     *
     * <p>This method is a DataDog extension, and may not work with other servers.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.
     *
     * @param aspect the name of the set
     * @param value the value to track
     * @param cardinality specify cardinality for Kubernetes or Docker tags added by the Agent
     * @param tags array of tags to be added to the data
     * @see <a
     *     href="http://docs.datadoghq.com/guides/dogstatsd/#sets">http://docs.datadoghq.com/guides/dogstatsd/#sets</a>
     */
    void recordSetValue(String aspect, String value, TagsCardinality cardinality, String... tags);
}
