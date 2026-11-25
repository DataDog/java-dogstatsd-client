/* Unless explicitly stated otherwise all files in this repository are
 * licensed under the Apache 2.0 License.
 *
 * This product includes software developed at Datadog
 *  (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.
 */

package com.datadoghq.dogstatsd.http.serializer;

import java.nio.BufferOverflowException;

/**
 * Build metrics payloads in a format accepted by the agent or the intake.
 *
 * <p>If any {@link Metric} method throws, the in-progress metric is no longer valid; the caller
 * must call {@link #resetMetric()} to discard the partial state before encoding any further
 * metrics. Metrics that are ended without any successfully-added points are silently dropped from
 * the payload.
 */
public class PayloadBuilder extends BasePayloadBuilder {
    final LongBuffer timestamps = new LongBuffer();
    final LongBuffer counts = new LongBuffer();

    final DeltaEncoder timestampsDelta = new DeltaEncoder();

    /**
     * Create a new PayloadBuilder.
     *
     * @param consumer Is given payloads one by one as they are finished.
     */
    public PayloadBuilder(PayloadConsumer consumer) {
        super(consumer);
    }

    /**
     * Begin encoding a new count metric.
     *
     * <p>Only one metric can be encoded at a time.
     *
     * @param name Name of the metric.
     * @return New builder instance.
     * @throws BufferOverflowException if finishing the previous metric overflows the payload.
     */
    public ScalarMetric count(String name) {
        ScalarMetric m = new ScalarMetric(this, 1, name);
        beginMetric(m);
        return m;
    }

    /**
     * Begin encoding a new rate metric.
     *
     * <p>Only one metric can be encoded at a time.
     *
     * @param name Name of the metric.
     * @return New builder instance.
     * @throws BufferOverflowException if finishing the previous metric overflows the payload.
     */
    public ScalarMetric rate(String name) {
        ScalarMetric m = new ScalarMetric(this, 2, name);
        beginMetric(m);
        return m;
    }

    /**
     * Begin encoding a new gauge metric.
     *
     * <p>Only one metric can be encoded at a time.
     *
     * @param name Name of the metric.
     * @return New builder instance.
     * @throws BufferOverflowException if finishing the previous metric overflows the payload.
     */
    public ScalarMetric gauge(String name) {
        ScalarMetric m = new ScalarMetric(this, 3, name);
        beginMetric(m);
        return m;
    }

    /**
     * Begin encoding a new sketch metric.
     *
     * <p>Only one metric can be encoded at a time.
     *
     * @param name Name of the metric.
     * @return New builder instance.
     * @throws BufferOverflowException if finishing the previous metric overflows the payload.
     */
    public SketchMetric sketch(String name) {
        SketchMetric m = new SketchMetric(this, 4, name);
        beginMetric(m);
        return m;
    }

    @Override
    void commitState() {
        super.commitState();
        timestampsDelta.commit();
    }

    @Override
    void revertState() {
        super.revertState();
        timestampsDelta.revert();
    }

    @Override
    void clearState() {
        super.clearState();
        timestampsDelta.clear();
    }

    @Override
    void clearMetric() {
        super.clearMetric();
        timestamps.clear();
        counts.clear();
    }

    void encodeTimestamps() {
        for (int i = 0; i < timestamps.length(); i++) {
            record.putSint64(Column.timestamps, timestampsDelta.encode(timestamps.get(i)));
        }
    }
}
