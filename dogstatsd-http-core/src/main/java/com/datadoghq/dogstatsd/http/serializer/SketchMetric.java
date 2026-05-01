/* Unless explicitly stated otherwise all files in this repository are
 * licensed under the Apache 2.0 License.
 *
 * This product includes software developed at Datadog
 *  (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.
 */

package com.datadoghq.dogstatsd.http.serializer;

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
     * Add a new timeseries point.
     *
     * @param timestamp Timestamp of the point in seconds since Unix epoch.
     * @param sum Total sum of all observed values.
     * @param min Minimum observed value.
     * @param max Maximum observed value.
     * @param cnt Number of observed values.
     * @param binKeys Array of keys for each bin in the sketch.
     * @param binCnts Array of number of observations for each bin.
     * @return This.
     */
    public SketchMetric addPoint(
            long timestamp,
            double sum,
            double min,
            double max,
            long cnt,
            int[] binKeys,
            int[] binCnts) {

        if (binKeys.length != binCnts.length) {
            throw new IllegalArgumentException("binKeys and binCnts must have the same length");
        }

        pb.timestamps.put(timestamp);
        pb.values.put(sum);
        pb.values.put(min);
        pb.values.put(max);
        pb.counts.put(cnt);

        ColumnarBuffer r = pb.currentRecord();
        DeltaEncoder dk = new DeltaEncoder();

        r.putUint64(Column.sketchNumBins, binKeys.length);
        for (int i = 0; i < binKeys.length; i++) {
            r.putSint64(Column.sketchBinKeys, dk.encode(binKeys[i]));
            r.putUint64(Column.sketchBinCnts, binCnts[i]);
        }

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
