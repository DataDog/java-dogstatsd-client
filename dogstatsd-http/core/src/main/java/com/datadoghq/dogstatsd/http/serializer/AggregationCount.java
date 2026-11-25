/* Unless explicitly stated otherwise all files in this repository are
 * licensed under the Apache 2.0 License.
 *
 * This product includes software developed at Datadog
 *  (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.
 */

package com.datadoghq.dogstatsd.http.serializer;

/** Builder for an aggregation count metric. */
public class AggregationCount extends Metric<AggregationCount> {
    private boolean hasValue;
    private double value;

    AggregationCount(AggregationPayloadBuilder pb, String name) {
        super(pb, MetricType.COUNT, name);
    }

    @Override
    AggregationCount self() {
        return this;
    }

    /**
     * Set the value of the count, replacing any previous value.
     *
     * @param sum Sum of the counts submitted during the window.
     * @return This.
     */
    public AggregationCount setValue(double sum) {
        this.value = sum;
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
        pb.currentRecord().putValue(valueType, value);
    }
}
