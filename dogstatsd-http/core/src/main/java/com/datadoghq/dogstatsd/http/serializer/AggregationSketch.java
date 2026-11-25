/* Unless explicitly stated otherwise all files in this repository are
 * licensed under the Apache 2.0 License.
 *
 * This product includes software developed at Datadog
 *  (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.
 */

package com.datadoghq.dogstatsd.http.serializer;

import com.datadoghq.dogstatsd.Sketch;
import java.nio.BufferOverflowException;

/**
 * Builder for an aggregation sketch (distribution) metric.
 *
 * <p>The sketch must use the agent's default DDSketch mapping and have no more than 4096 bins.
 */
public class AggregationSketch extends Metric<AggregationSketch> {
    private boolean hasSketch;
    private double sum;
    private double min;
    private double max;
    private long count;

    AggregationSketch(AggregationPayloadBuilder pb, String name) {
        super(pb, MetricType.SKETCH, name);
    }

    @Override
    AggregationSketch self() {
        return this;
    }

    /**
     * Set the sketch of the window.
     *
     * @param sketch Sketch supplying the summary statistics and bin distribution.
     * @return This.
     * @throws IllegalStateException if a sketch was already set for this metric.
     * @throws BufferOverflowException if the sketch's bin data alone would exceed the maximum
     *     payload size. The metric is left unchanged and remains valid.
     */
    public AggregationSketch setSketch(Sketch sketch) {
        if (hasSketch) {
            throw new IllegalStateException("sketch already set");
        }
        SketchBinConsumer.putBins(pb.currentRecord(), pb.maxPayloadSize, sketch);
        sum = sketch.sum();
        min = sketch.min();
        max = sketch.max();
        count = sketch.count();
        hasSketch = true;
        return this;
    }

    @Override
    boolean hasData() {
        return hasSketch && count > 0;
    }

    @Override
    ValueType valueType() {
        return PointKind.of(sum).union(PointKind.of(min)).union(PointKind.of(max)).toValueType();
    }

    @Override
    void encodeValues(ValueType valueType) {
        ColumnarBuffer r = pb.currentRecord();
        r.putValue(valueType, sum);
        r.putValue(valueType, min);
        r.putValue(valueType, max);
        r.putSint64(Column.valsSint64, count);
    }
}
