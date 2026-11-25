/* Unless explicitly stated otherwise all files in this repository are
 * licensed under the Apache 2.0 License.
 *
 * This product includes software developed at Datadog
 *  (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.
 */

package com.datadoghq.dogstatsd.http.serializer;

import java.util.List;

/**
 * Common settings shared by all timeseries builders.
 *
 * <p>Instances are obtained from {@link DirectPayloadBuilder} or {@link AggregationPayloadBuilder};
 * this class cannot be subclassed outside of this package.
 *
 * @param <T> the concrete builder type returned by the setters, for chaining.
 */
public abstract class Metric<T extends Metric<T>> {
    final BasePayloadBuilder pb;

    final long type;
    final String name;
    List<String> tags = null;
    List<String> resources = null;
    TagsCardinality cardinality = TagsCardinality.DEFAULT;
    Origin origin = Origin.dogstatsd;

    Metric(BasePayloadBuilder pb, int type, String name) {
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
     * @throws IllegalArgumentException if {@code resources} has an odd number of elements.
     */
    public T setResources(List<String> resources) {
        if (resources != null && resources.size() % 2 != 0) {
            throw new IllegalArgumentException("resources must contain even number of elements");
        }
        this.resources = resources;
        return self();
    }

    /**
     * Set the cardinality of the origin tags the agent attaches to this metric.
     *
     * <p>Ignored by the intake, which does not attach origin tags.
     *
     * @param cardinality The cardinality to request, or null for {@link TagsCardinality#DEFAULT}.
     * @return This.
     */
    public T setTagsCardinality(TagsCardinality cardinality) {
        this.cardinality = cardinality == null ? TagsCardinality.DEFAULT : cardinality;
        return self();
    }

    abstract T self();

    /** Whether the metric has anything to encode; metrics without data are dropped. */
    abstract boolean hasData();

    /** Value column used by this metric, selected from its buffered values. */
    abstract ValueType valueType();

    abstract void encodeValues(ValueType valueType);

    void encodeIndependentFields() {
        ColumnarBuffer r = pb.currentRecord();
        ValueType valueType = valueType();
        r.putUint64(Column.types, type | valueType.flag() | cardinality.flag());
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
        r.clear(Column.dictOriginInfo);
        r.clear(Column.originInfoRefs);
    }

    void encodeDependentFields() {
        pb.encodeName(name);
        pb.encodeTags(tags);
        pb.encodeResources(resources);
        pb.encodeOrigin(origin);
    }

    /**
     * Finish this timeseries and add it to the payload.
     *
     * @throws java.nio.BufferOverflowException if the encoded metric exceeds the maximum payload
     *     size. The in-progress metric is discarded, so no {@code resetMetric()} call is needed
     *     before encoding further metrics.
     */
    public void close() {
        pb.endMetric();
    }
}
