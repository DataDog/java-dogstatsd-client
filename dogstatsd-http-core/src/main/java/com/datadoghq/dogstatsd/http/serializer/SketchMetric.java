/* Unless explicitly stated otherwise all files in this repository are
 * licensed under the Apache 2.0 License.
 *
 * This product includes software developed at Datadog
 *  (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.
 */

package com.datadoghq.dogstatsd.http.serializer;

import com.datadoghq.dogstatsd.Sketch;
import java.nio.BufferOverflowException;

/** Builder for sketch timeseries. */
public class SketchMetric extends Metric<SketchMetric> {

    SketchMetric(PayloadBuilder pb, int type, String name) {
        super(pb, type, name);
    }

    @Override
    protected SketchMetric self() {
        return this;
    }

    /**
     * Add a new timeseries point sourced from a {@link Sketch}.
     *
     * @param timestamp Timestamp of the point in seconds since Unix epoch.
     * @param sketch Sketch supplying the summary statistics and bin distribution.
     * @return This.
     */
    public SketchMetric addPoint(long timestamp, Sketch sketch) {
        final long maxBinCount = (1L << 32) - 1;
        final long maxBinBytes = ProtoUtil.varintLen(maxBinCount);

        // Skip doing the work if just the bin data would exceed payload size limit.
        if (sketch.count() / maxBinCount * maxBinBytes >= pb.maxPayloadSize) {
            throw new BufferOverflowException();
        }

        pb.timestamps.put(timestamp);
        pb.values.put(sketch.sum());
        pb.values.put(sketch.min());
        pb.values.put(sketch.max());
        pb.counts.put(sketch.count());

        final ColumnarBuffer r = pb.currentRecord();
        final DeltaEncoder dk = new DeltaEncoder();

        r.putUint64(Column.sketchNumBins, sketch.size());
        sketch.bins(
                new Sketch.BinConsumer() {
                    @Override
                    public void consumeBin(short key, long count) {
                        while (count > maxBinCount) {
                            r.putSint64(Column.sketchBinKeys, dk.encode(key));
                            r.putUint64(Column.sketchBinCnts, maxBinCount);
                            count -= maxBinCount;
                        }
                        r.putSint64(Column.sketchBinKeys, dk.encode(key));
                        r.putUint64(Column.sketchBinCnts, count);
                    }
                });

        return this;
    }

    private static final int VALUES_PER_SKETCH_POINT = 3;

    @Override
    void encodeValues(ValueType valueType) {
        ColumnarBuffer r = pb.currentRecord();

        r.putUint64(Column.numPoints, pb.counts.length());

        switch (valueType) {
            case zero:
                for (int i = 0; i < pb.counts.length(); i++) {
                    r.putSint64(Column.valsSint64, pb.counts.get(i));
                }
                break;
            case sint64:
                for (int i = 0; i < pb.counts.length(); i++) {
                    r.putSint64(
                            Column.valsSint64, (long) pb.values.get(VALUES_PER_SKETCH_POINT * i));
                    r.putSint64(
                            Column.valsSint64,
                            (long) pb.values.get(VALUES_PER_SKETCH_POINT * i + 1));
                    r.putSint64(
                            Column.valsSint64,
                            (long) pb.values.get(VALUES_PER_SKETCH_POINT * i + 2));
                    r.putSint64(Column.valsSint64, pb.counts.get(i));
                }
                break;
            case float32:
                for (int i = 0; i < pb.counts.length(); i++) {
                    r.putFloat32(
                            Column.valsFloat32, (float) pb.values.get(VALUES_PER_SKETCH_POINT * i));
                    r.putFloat32(
                            Column.valsFloat32,
                            (float) pb.values.get(VALUES_PER_SKETCH_POINT * i + 1));
                    r.putFloat32(
                            Column.valsFloat32,
                            (float) pb.values.get(VALUES_PER_SKETCH_POINT * i + 2));
                    r.putSint64(Column.valsSint64, pb.counts.get(i));
                }
                break;
            case float64:
                for (int i = 0; i < pb.counts.length(); i++) {
                    r.putFloat64(Column.valsFloat64, pb.values.get(VALUES_PER_SKETCH_POINT * i));
                    r.putFloat64(
                            Column.valsFloat64, pb.values.get(VALUES_PER_SKETCH_POINT * i + 1));
                    r.putFloat64(
                            Column.valsFloat64, pb.values.get(VALUES_PER_SKETCH_POINT * i + 2));
                    r.putSint64(Column.valsSint64, pb.counts.get(i));
                }
                break;
        }
    }
}
