/* Unless explicitly stated otherwise all files in this repository are
 * licensed under the Apache 2.0 License.
 *
 * This product includes software developed at Datadog
 *  (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.
 */

package com.datadoghq.dogstatsd.http.serializer;

/** Builder for scalar timeseries. */
public class ScalarMetric extends Metric<ScalarMetric> {
    ScalarMetric(PayloadBuilder pb, int type, String name) {
        super(pb, type, name);
    }

    @Override
    protected ScalarMetric self() {
        return this;
    }

    /**
     * Add new data point to the timeseries.
     *
     * @param timestamp Timestamp of the point in seconds since Unix epoch.
     * @param value Metric value at timestamp.
     * @return This.
     */
    public ScalarMetric addPoint(long timestamp, double value) {
        pb.timestamps.put(timestamp);
        pb.values.put(value);
        return this;
    }

    @Override
    void encodeValues(ValueType valueType) {
        ColumnarBuffer r = pb.currentRecord();

        r.putUint64(Column.numPoints, pb.values.length());

        switch (valueType) {
            case zero:
                break;
            case sint64:
                for (int i = 0; i < pb.values.length(); i++) {
                    r.putSint64(Column.valsSint64, (long) pb.values.get(i));
                }
                break;
            case float32:
                for (int i = 0; i < pb.values.length(); i++) {
                    r.putFloat32(Column.valsFloat32, (float) pb.values.get(i));
                }
                break;
            case float64:
                for (int i = 0; i < pb.values.length(); i++) {
                    r.putFloat64(Column.valsFloat64, pb.values.get(i));
                }
                break;
        }
    }
}
