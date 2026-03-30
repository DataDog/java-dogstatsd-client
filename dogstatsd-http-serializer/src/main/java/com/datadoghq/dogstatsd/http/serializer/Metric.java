/* Unless explicitly stated otherwise all files in this repository are
 * licensed under the Apache 2.0 License.
 *
 * This product includes software developed at Datadog
 *  (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.
 */

package com.datadoghq.dogstatsd.http.serializer;

import java.util.List;

abstract class Metric<T extends Metric<T>> {
    final PayloadBuilder pb;

    final long type;
    final String name;
    List<String> tags = null;
    List<String> resources = null;
    int interval = 0;
    Origin origin = Origin.dogstatsd;

    Metric(PayloadBuilder pb, int type, String name) {
        this.pb = pb;
        this.type = type;
        this.name = name;
    }

    /**
     * Set tags for this metric.
     *
     * @param tags List of tags to apply to this metric, or null for no tags.
     * @return This.
     */
    public T setTags(List<String> tags) {
        this.tags = tags;
        return self();
    }

    /**
     * Set resources for this metric.
     *
     * @param resources List of even length, containing zero or more (type, name) pairs, or null for
     *     no resources.
     * @return This.
     */
    public T setResources(List<String> resources) {
        if (resources != null && resources.size() % 2 != 0) {
            throw new IllegalArgumentException("resources must contain even number of elements");
        }
        this.resources = resources;
        return self();
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

    abstract T self();

    abstract void encodeValues(ValueType valueType);

    void encodeIndependentFields() {
        ColumnarBuffer r = pb.currentRecord();
        ValueType valueType = PointKind.of(pb.values).toValueType();
        r.putUint64(Column.types, type | valueType.flag());
        r.putUint64(Column.intervals, interval);
        r.putSint64(Column.sourceTypeNameRefs, 0);
        encodeValues(valueType);
    }

    void clearDependentFields() {
        ColumnarBuffer r = pb.currentRecord();
        r.clear(Column.dictNameStr);
        r.clear(Column.nameRefs);
        r.clear(Column.dictTagStr);
        r.clear(Column.dictTagsets);
        r.clear(Column.tagsetRefs);
        r.clear(Column.dictResourceStr);
        r.clear(Column.dictResourceLen);
        r.clear(Column.dictResourceType);
        r.clear(Column.dictResourceName);
        r.clear(Column.resourcesRefs);
        r.clear(Column.timestamps);
        r.clear(Column.dictOriginInfo);
        r.clear(Column.originInfoRefs);
    }

    void encodeDependentFields() {
        pb.encodeName(name);
        pb.encodeTags(tags);
        pb.encodeResources(resources);
        pb.encodeTimestamps();
        pb.encodeOrigin(origin);
    }

    /** Finish this timeseries and add it to the payload. */
    public void close() {
        pb.endMetric();
    }
}
