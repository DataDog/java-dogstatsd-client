/* Unless explicitly stated otherwise all files in this repository are
 * licensed under the Apache 2.0 License.
 *
 * This product includes software developed at Datadog
 *  (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.
 */

package com.datadoghq.dogstatsd.http.serializer;

/**
 * Builder for an aggregation histogram metric.
 *
 * <p>Carries the distinct values observed during the window, each with its weight. Timings are sent
 * as histograms.
 */
public class AggregationHistogram extends Metric<AggregationHistogram> {
    private double lastValue;

    AggregationHistogram(AggregationPayloadBuilder pb, String name) {
        super(pb, MetricType.HISTOGRAM, name);
    }

    @Override
    AggregationHistogram self() {
        return this;
    }

    /**
     * Add a sample to the histogram.
     *
     * <p>Samples must be added in strictly ascending order of value.
     *
     * @param value Observed value.
     * @param weight Weight of the value, the sum of {@code 1/sample_rate} of its observations; must
     *     be finite and greater than zero.
     * @return This.
     * @throws IllegalArgumentException if {@code value} is NaN, or is not greater than the value of
     *     the previous sample. The metric is left unchanged and remains valid.
     */
    public AggregationHistogram addSample(double value, double weight) {
        if (Double.isNaN(value)) {
            throw new IllegalArgumentException("histogram value must not be NaN");
        }
        if (pb.values.length() > 0 && !(value > lastValue)) {
            throw new IllegalArgumentException(
                    "histogram values must be distinct and added in ascending order");
        }
        pb.values.put(value);
        pb.values.put(weight);
        lastValue = value;
        return this;
    }

    @Override
    boolean hasData() {
        return pb.values.length() > 0;
    }

    @Override
    ValueType valueType() {
        ValueType vt = PointKind.of(pb.values).toValueType();
        // Weights are > 0, so this is only hit when the caller breaks that rule. The format does
        // not allow Zero for histograms either way.
        return vt == ValueType.zero ? ValueType.sint64 : vt;
    }

    @Override
    void encodeValues(ValueType valueType) {
        ColumnarBuffer r = pb.currentRecord();
        r.putUint64(Column.valsUint64, pb.values.length() / 2);
        for (int i = 0; i < pb.values.length(); i++) {
            r.putValue(valueType, pb.values.get(i));
        }
    }
}
