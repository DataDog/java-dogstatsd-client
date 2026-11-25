/* Unless explicitly stated otherwise all files in this repository are
 * licensed under the Apache 2.0 License.
 *
 * This product includes software developed at Datadog
 *  (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.
 */

package com.datadoghq.dogstatsd.http.serializer;

/**
 * Settings shared by the timeseries builders of {@link DirectPayloadBuilder}, whose metrics carry
 * timestamped points.
 *
 * @param <T> the concrete builder type returned by the setters, for chaining.
 */
public abstract class DirectMetric<T extends DirectMetric<T>> extends Metric<T> {
    // Hides Metric.pb with the same builder, typed to reach the timestamp and count buffers.
    final DirectPayloadBuilder pb;
    int interval = 0;

    DirectMetric(DirectPayloadBuilder pb, int type, String name) {
        super(pb, type, name);
        this.pb = pb;
    }

    /**
     * Set aggregation interval setting for this metric.
     *
     * @param interval Aggregation interval in seconds.
     * @return This.
     */
    public T setInterval(int interval) {
        this.interval = interval;
        return self();
    }

    @Override
    boolean hasData() {
        return pb.timestamps.length() > 0;
    }

    @Override
    ValueType valueType() {
        return PointKind.of(pb.values).toValueType();
    }

    @Override
    void encodeIndependentFields() {
        super.encodeIndependentFields();
        ColumnarBuffer r = pb.currentRecord();
        r.putUint64(Column.intervals, interval);
        r.putSint64(Column.sourceTypeNameRefs, 0);
    }

    @Override
    void clearDependentFields() {
        super.clearDependentFields();
        pb.currentRecord().clear(Column.timestamps);
    }

    @Override
    void encodeDependentFields() {
        super.encodeDependentFields();
        pb.encodeTimestamps();
    }
}
