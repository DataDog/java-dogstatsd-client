/* Unless explicitly stated otherwise all files in this repository are
 * licensed under the Apache 2.0 License.
 *
 * This product includes software developed at Datadog
 *  (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.
 */

package com.datadoghq.dogstatsd.http.serializer;

import java.nio.BufferOverflowException;

/**
 * Build payloads for the agent's aggregation endpoint.
 *
 * <p>A payload carries the intermediate series one client aggregated over one window. Each metric
 * has a single value for the whole window, so metrics have no timestamps or intervals: the window
 * timestamp and duration are sent out of band, in the request URL.
 *
 * <p>The caller must add metrics in primary key order: tags cardinality, then tagset, then
 * resources, then name, with each key occurring once. Strings compare by UTF-8 length, then
 * bytewise. Tagsets and resource sets compare by length, then element-wise in ascending order;
 * resources compare by type, then name. The builder does not check this order.
 *
 * <p>If any metric method throws, the in-progress metric is no longer valid, unless the method
 * documents otherwise; the caller must call {@link #resetMetric()} to discard the partial state
 * before encoding any further metrics. Metrics that are ended without a value are silently dropped
 * from the payload.
 */
public class AggregationPayloadBuilder extends BasePayloadBuilder {
    /**
     * Create a new AggregationPayloadBuilder.
     *
     * @param consumer Is given payloads one by one as they are finished.
     */
    public AggregationPayloadBuilder(PayloadConsumer consumer) {
        super(consumer);
    }

    /**
     * Begin encoding a new aggregation count metric.
     *
     * <p>Only one metric can be encoded at a time.
     *
     * @param name Name of the metric.
     * @return New builder instance.
     * @throws BufferOverflowException if finishing the previous metric overflows the payload.
     */
    public AggregationCount count(String name) {
        AggregationCount m = new AggregationCount(this, name);
        beginMetric(m);
        return m;
    }

    /**
     * Begin encoding a new aggregation mergeable gauge metric.
     *
     * <p>Only one metric can be encoded at a time.
     *
     * @param name Name of the metric.
     * @return New builder instance.
     * @throws BufferOverflowException if finishing the previous metric overflows the payload.
     */
    public AggregationMergeableGauge mergeableGauge(String name) {
        AggregationMergeableGauge m = new AggregationMergeableGauge(this, name);
        beginMetric(m);
        return m;
    }

    /**
     * Begin encoding a new aggregation histogram metric.
     *
     * <p>Only one metric can be encoded at a time.
     *
     * @param name Name of the metric.
     * @return New builder instance.
     * @throws BufferOverflowException if finishing the previous metric overflows the payload.
     */
    public AggregationHistogram histogram(String name) {
        AggregationHistogram m = new AggregationHistogram(this, name);
        beginMetric(m);
        return m;
    }

    /**
     * Begin encoding a new aggregation set metric.
     *
     * <p>Only one metric can be encoded at a time.
     *
     * @param name Name of the metric.
     * @return New builder instance.
     * @throws BufferOverflowException if finishing the previous metric overflows the payload.
     */
    public AggregationSet set(String name) {
        AggregationSet m = new AggregationSet(this, name);
        beginMetric(m);
        return m;
    }

    /**
     * Begin encoding a new aggregation sketch (distribution) metric.
     *
     * <p>Only one metric can be encoded at a time.
     *
     * @param name Name of the metric.
     * @return New builder instance.
     * @throws BufferOverflowException if finishing the previous metric overflows the payload.
     */
    public AggregationSketch sketch(String name) {
        AggregationSketch m = new AggregationSketch(this, name);
        beginMetric(m);
        return m;
    }
}
