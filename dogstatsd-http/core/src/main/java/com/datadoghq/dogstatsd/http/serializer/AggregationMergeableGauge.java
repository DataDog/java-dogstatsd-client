/* Unless explicitly stated otherwise all files in this repository are
 * licensed under the Apache 2.0 License.
 *
 * This product includes software developed at Datadog
 *  (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.
 */

package com.datadoghq.dogstatsd.http.serializer;

/**
 * Builder for an aggregation mergeable gauge metric.
 *
 * <p>Carries the latest measurement of the window along with its time, so that the receiver can
 * merge gauges from several clients by keeping the latest one.
 */
public class AggregationMergeableGauge extends Metric<AggregationMergeableGauge> {
    private boolean hasValue;
    private double value;
    private long offsetMillis;

    AggregationMergeableGauge(AggregationPayloadBuilder pb, String name) {
        super(pb, MetricType.MERGEABLE_GAUGE, name);
    }

    @Override
    AggregationMergeableGauge self() {
        return this;
    }

    /**
     * Set the value of the gauge, replacing any previous value.
     *
     * @param value Latest measurement of the window.
     * @param offsetMillis Time of the latest measurement as an offset from the window start, in
     *     milliseconds; must be non-negative and less than the window duration.
     * @return This.
     */
    public AggregationMergeableGauge setValue(double value, long offsetMillis) {
        this.value = value;
        this.offsetMillis = offsetMillis;
        this.hasValue = true;
        return this;
    }

    @Override
    boolean hasData() {
        return hasValue;
    }

    @Override
    ValueType valueType() {
        return PointKind.of(value).toValueType();
    }

    @Override
    void encodeValues(ValueType valueType) {
        ColumnarBuffer r = pb.currentRecord();
        r.putValue(valueType, value);
        r.putUint64(Column.valsUint64, offsetMillis);
    }
}
