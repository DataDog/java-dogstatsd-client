/* Unless explicitly stated otherwise all files in this repository are
 * licensed under the Apache 2.0 License.
 *
 * This product includes software developed at Datadog
 *  (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.
 */

package com.datadoghq.dogstatsd.http.serializer;

import java.nio.BufferOverflowException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** Build metrics payloads in a format accepted by the agent or the intake. */
public class PayloadBuilder {
    private static final int DEFAULT_MAX_PAYLOAD_SIZE = 256 * 1024;
    private static final int METRIC_DATA_FIELD_ID = 3;

    final int maxPayloadSize;

    final ColumnarBuffer payload = new ColumnarBuffer();
    final ColumnarBuffer record = new ColumnarBuffer();
    final LongBuffer timestamps = new LongBuffer();
    final DoubleBuffer values = new DoubleBuffer();
    final LongBuffer counts = new LongBuffer();

    final PayloadConsumer consumer;

    final Interner<String> nameStrInterner =
            new Interner<String>(
                    "",
                    new Interner.Encoder<String>() {
                        @Override
                        public void encode(String s) {
                            record.putString(Column.dictNameStr, s);
                        }
                    });

    final Interner<String> tagStrInterner =
            new Interner<String>(
                    "",
                    new Interner.Encoder<String>() {
                        @Override
                        public void encode(String s) {
                            record.putString(Column.dictTagStr, s);
                        }
                    });

    final Interner<List<String>> tagsInterner =
            new Interner<List<String>>(
                    Collections.<String>emptyList(),
                    new Interner.Encoder<List<String>>() {
                        String[] strBuf = new String[0];
                        LongBuffer idBuf = new LongBuffer();

                        @Override
                        public void encode(List<String> val) {
                            int size = val.size();
                            strBuf = val.toArray(strBuf);
                            Arrays.sort(strBuf, 0, size);

                            idBuf.clear();
                            for (int i = 0; i < size; i++) {
                                idBuf.put(tagStrInterner.intern(strBuf[i]));
                            }
                            idBuf.sort();
                            Arrays.fill(strBuf, null);

                            DeltaEncoder enc = new DeltaEncoder();

                            record.putSint64(Column.dictTagsets, size);
                            for (int i = 0; i < size; i++) {
                                record.putSint64(Column.dictTagsets, enc.encode(idBuf.get(i)));
                            }
                        }
                    });

    final Interner<String> resourceStrInterner =
            new Interner<String>(
                    "",
                    new Interner.Encoder<String>() {
                        @Override
                        public void encode(String s) {
                            record.putString(Column.dictResourceStr, s);
                        }
                    });

    final Interner<List<String>> resourcesInterner =
            new Interner<List<String>>(
                    Collections.<String>emptyList(),
                    new Interner.Encoder<List<String>>() {
                        @Override
                        public void encode(List<String> val) {
                            int size = val.size() / 2;
                            DeltaEncoder dt = new DeltaEncoder();
                            DeltaEncoder dn = new DeltaEncoder();
                            record.putUint64(Column.dictResourceLen, size);
                            for (int i = 0; i < size; i++) {
                                record.putSint64(
                                        Column.dictResourceType,
                                        dt.encode(resourceStrInterner.intern(val.get(2 * i))));
                                record.putSint64(
                                        Column.dictResourceName,
                                        dn.encode(resourceStrInterner.intern(val.get(2 * i + 1))));
                            }
                        }
                    });

    final Interner<Origin> originInfoInterner =
            new Interner<Origin>(
                    Origin.undefined,
                    new Interner.Encoder<Origin>() {
                        @Override
                        public void encode(Origin o) {
                            record.putUint64(Column.dictOriginInfo, o.product);
                            record.putUint64(Column.dictOriginInfo, o.category);
                            record.putUint64(Column.dictOriginInfo, o.service);
                        }
                    });

    final DeltaEncoder nameRefsDelta = new DeltaEncoder();
    final DeltaEncoder tagsetRefsDelta = new DeltaEncoder();
    final DeltaEncoder resourcesRefsDelta = new DeltaEncoder();
    final DeltaEncoder originInfoRefsDelta = new DeltaEncoder();
    final DeltaEncoder timestampsDelta = new DeltaEncoder();

    Metric<?> metricInProgress;

    /**
     * Create new PayloadBuilder.
     *
     * @param consumer Is given payloads one by one as they are finished.
     */
    public PayloadBuilder(PayloadConsumer consumer) {
        this.consumer = consumer;
        this.maxPayloadSize = DEFAULT_MAX_PAYLOAD_SIZE;
    }

    /**
     * Begin encoding new count metric.
     *
     * <p>Only one metric can be encoded at a time.
     *
     * @param name Name of the metric.
     * @return Builder instance.
     */
    public ScalarMetric count(String name) {
        ScalarMetric m = new ScalarMetric(this, 1, name);
        beginMetric(m);
        return m;
    }

    /**
     * Begin encoding new rate metric.
     *
     * <p>Only one metric can be encoded at a time.
     *
     * @param name Name of the metric.
     * @return New builder instance.
     */
    public ScalarMetric rate(String name) {
        ScalarMetric m = new ScalarMetric(this, 2, name);
        beginMetric(m);
        return m;
    }

    /**
     * Begin encoding new gauge metric.
     *
     * <p>Only one metric can be encoded at a time.
     *
     * @param name Name of the metric.
     * @return New builder instance.
     */
    public ScalarMetric gauge(String name) {
        ScalarMetric m = new ScalarMetric(this, 3, name);
        beginMetric(m);
        return m;
    }

    /**
     * Begin encoding new sketch metric.
     *
     * <p>Only one metric can be encoded at a time.
     *
     * @param name Name of the metric.
     * @return New builder instance.
     */
    public SketchMetric sketch(String name) {
        SketchMetric m = new SketchMetric(this, 4, name);
        beginMetric(m);
        return m;
    }

    void beginMetric(Metric m) {
        endMetric();
        metricInProgress = m;
    }

    void endMetric() {
        Metric m = metricInProgress;
        if (m == null) {
            return;
        }

        try {
            m.encodeIndependentFields();
            m.encodeDependentFields();

            if (record.length() > maxPayloadSize) {
                throw new BufferOverflowException();
            }

            if (payload.length() + record.length() > maxPayloadSize) {
                flushPayload();
                // Flush clears interners and delta-encoders, so we need to re-encode some
                // columns.
                m.clearDependentFields();
                m.encodeDependentFields();
            }
            payload.put(record);
        } finally {
            record.clear();
            timestamps.clear();
            values.clear();
            counts.clear();
            metricInProgress = null;
        }
    }

    ColumnarBuffer currentRecord() {
        return record;
    }

    void encodeName(String name) {
        long id = nameStrInterner.intern(name);
        record.putSint64(Column.nameRefs, nameRefsDelta.encode(id));
    }

    void encodeTimestamps() {
        for (int i = 0; i < timestamps.length(); i++) {
            record.putSint64(Column.timestamps, timestampsDelta.encode(timestamps.get(i)));
        }
    }

    void encodeTags(List<String> tags) {
        long id = tagsInterner.intern(tags);
        record.putSint64(Column.tagsetRefs, tagsetRefsDelta.encode(id));
    }

    void encodeResources(List<String> resources) {
        long id = resourcesInterner.intern(resources);
        record.putSint64(Column.resourcesRefs, resourcesRefsDelta.encode(id));
    }

    void encodeOrigin(Origin origin) {
        long id = originInfoInterner.intern(origin);
        record.putSint64(Column.originInfoRefs, originInfoRefsDelta.encode(id));
    }

    void flushPayload() {
        int dataLen = payload.length();
        if (dataLen > 0) {
            ByteBuffer p = new ByteBuffer(ProtoUtil.fieldLen(METRIC_DATA_FIELD_ID, dataLen));
            p.putBytesFieldHeader(METRIC_DATA_FIELD_ID, dataLen);
            payload.renderProtobufTo(p);
            consumer.handle(p.toArray());
        }

        payload.clear();

        nameStrInterner.clear();
        tagStrInterner.clear();
        tagsInterner.clear();
        resourceStrInterner.clear();
        resourcesInterner.clear();
        originInfoInterner.clear();

        nameRefsDelta.clear();
        tagsetRefsDelta.clear();
        resourcesRefsDelta.clear();
        originInfoRefsDelta.clear();
        timestampsDelta.clear();
    }

    /** Finish any pending data. */
    public void close() {
        endMetric();
        flushPayload();
    }
}
