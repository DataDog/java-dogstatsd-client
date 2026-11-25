/* Unless explicitly stated otherwise all files in this repository are
 * licensed under the Apache 2.0 License.
 *
 * This product includes software developed at Datadog
 *  (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.
 */

package com.datadoghq.dogstatsd.http.serializer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.datadoghq.dogstatsd.Sketch;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;

public class AggregationPayloadBuilderTest {
    final List<byte[]> payloads = new ArrayList<>();
    final AggregationPayloadBuilder b =
            new AggregationPayloadBuilder(
                    new PayloadConsumer() {
                        @Override
                        public void handle(byte[] p) {
                            payloads.add(p);
                        }
                    });

    /** Columns of one MetricData message, decoded independently of the encoder. */
    static class Decoded {
        final Map<Integer, byte[]> cols = new HashMap<>();

        Decoded(byte[] payload) {
            int[] pos = {0};
            long tag = varint(payload, pos);
            assertEquals("Payload.metricData tag", (3 << 3) | 2, tag);
            int len = (int) varint(payload, pos);
            assertEquals("metricData is the whole payload", payload.length, pos[0] + len);
            while (pos[0] < payload.length) {
                long t = varint(payload, pos);
                assertEquals("every column is length-delimited", 2, t & 7);
                int flen = (int) varint(payload, pos);
                int id = (int) (t >> 3);
                assertNull("column " + id + " occurs at most once", cols.get(id));
                cols.put(id, Arrays.copyOfRange(payload, pos[0], pos[0] + flen));
                pos[0] += flen;
            }
        }

        static long varint(byte[] p, int[] pos) {
            long v = 0;
            for (int sh = 0; ; sh += 7) {
                byte x = p[pos[0]++];
                v |= (long) (x & 0x7f) << sh;
                if (x >= 0) {
                    return v;
                }
            }
        }

        boolean has(Column c) {
            return cols.containsKey(c.id);
        }

        byte[] col(Column c) {
            byte[] v = cols.get(c.id);
            return v == null ? new byte[0] : v;
        }

        long[] uints(Column c) {
            byte[] p = col(c);
            List<Long> out = new ArrayList<>();
            int[] pos = {0};
            while (pos[0] < p.length) {
                out.add(varint(p, pos));
            }
            long[] r = new long[out.size()];
            for (int i = 0; i < r.length; i++) r[i] = out.get(i);
            return r;
        }

        long[] sints(Column c) {
            long[] r = uints(c);
            for (int i = 0; i < r.length; i++) r[i] = (r[i] >>> 1) ^ -(r[i] & 1);
            return r;
        }

        /** Delta-decoded sint64 column. */
        long[] refs(Column c) {
            long[] r = sints(c);
            for (int i = 1; i < r.length; i++) r[i] += r[i - 1];
            return r;
        }

        float[] floats(Column c) {
            java.nio.ByteBuffer bb =
                    java.nio.ByteBuffer.wrap(col(c)).order(java.nio.ByteOrder.LITTLE_ENDIAN);
            float[] r = new float[bb.remaining() / 4];
            for (int i = 0; i < r.length; i++) r[i] = bb.getFloat();
            return r;
        }

        double[] doubles(Column c) {
            java.nio.ByteBuffer bb =
                    java.nio.ByteBuffer.wrap(col(c)).order(java.nio.ByteOrder.LITTLE_ENDIAN);
            double[] r = new double[bb.remaining() / 8];
            for (int i = 0; i < r.length; i++) r[i] = bb.getDouble();
            return r;
        }

        List<String> strs(Column c) {
            byte[] p = col(c);
            List<String> out = new ArrayList<>();
            int[] pos = {0};
            while (pos[0] < p.length) {
                int len = (int) varint(p, pos);
                out.add(new String(p, pos[0], len, UTF_8));
                pos[0] += len;
            }
            return out;
        }

        /** Metric name of every metric, resolved through the name dictionary. */
        List<String> names() {
            List<String> dict = strs(Column.dictNameStr);
            List<String> out = new ArrayList<>();
            for (long ref : refs(Column.nameRefs)) {
                out.add(ref == 0 ? "" : dict.get((int) ref - 1));
            }
            return out;
        }

        void assertNoPerPointColumns() {
            assertFalse("intervals", has(Column.intervals));
            assertFalse("numPoints", has(Column.numPoints));
            assertFalse("timestamps", has(Column.timestamps));
            assertFalse("sourceTypeNameRefs", has(Column.sourceTypeNameRefs));
        }
    }

    Decoded single() {
        b.close();
        assertEquals(1, payloads.size());
        Decoded d = new Decoded(payloads.get(0));
        d.assertNoPerPointColumns();
        return d;
    }

    @Test
    public void count() {
        b.count("a").setValue(5).close();
        b.count("b").setValue(0).close();
        b.count("c").setValue(-7).close();
        Decoded d = single();

        assertEquals(Arrays.asList("a", "b", "c"), d.names());
        assertArrayEquals(new long[] {0x11, 0x01, 0x11}, d.uints(Column.types));
        assertArrayEquals(new long[] {5, -7}, d.sints(Column.valsSint64));
        assertFalse(d.has(Column.valsUint64));
        assertArrayEquals(new long[] {0, 0, 0}, d.refs(Column.tagsetRefs));
        assertArrayEquals(new long[] {0, 0, 0}, d.refs(Column.resourcesRefs));
        assertArrayEquals(new long[] {1, 1, 1}, d.refs(Column.originInfoRefs));
        assertArrayEquals(new long[] {10, 10, 0}, d.uints(Column.dictOriginInfo));
    }

    @Test
    public void countFloat() {
        b.count("a").setValue(1.5).close();
        Decoded d = single();
        assertArrayEquals(new long[] {0x21}, d.uints(Column.types));
        assertArrayEquals(new float[] {1.5f}, d.floats(Column.valsFloat32), 0);
    }

    @Test
    public void countLastValueWins() {
        b.count("a").setValue(1).setValue(2).close();
        Decoded d = single();
        assertArrayEquals(new long[] {2}, d.sints(Column.valsSint64));
    }

    @Test
    public void mergeableGauge() {
        b.mergeableGauge("g").setValue(2.5, 1234).close();
        b.mergeableGauge("h").setValue(0.1, 0).close();
        Decoded d = single();

        assertEquals(Arrays.asList("g", "h"), d.names());
        assertArrayEquals(new long[] {0x28, 0x38}, d.uints(Column.types));
        assertArrayEquals(new float[] {2.5f}, d.floats(Column.valsFloat32), 0);
        assertArrayEquals(new double[] {0.1}, d.doubles(Column.valsFloat64), 0);
        assertArrayEquals(new long[] {1234, 0}, d.uints(Column.valsUint64));
    }

    @Test
    public void histogramIntegers() {
        b.histogram("h").addSample(-1, 1).addSample(2, 3).close();
        Decoded d = single();
        assertArrayEquals(new long[] {0x19}, d.uints(Column.types));
        assertArrayEquals(new long[] {2}, d.uints(Column.valsUint64));
        assertArrayEquals(new long[] {-1, 1, 2, 3}, d.sints(Column.valsSint64));
    }

    @Test
    public void histogramZeroIsNeverZeroType() {
        b.histogram("h").addSample(0, 0).close();
        Decoded d = single();
        assertArrayEquals(new long[] {0x19}, d.uints(Column.types));
        assertArrayEquals(new long[] {1}, d.uints(Column.valsUint64));
        assertArrayEquals(new long[] {0, 0}, d.sints(Column.valsSint64));
    }

    @Test
    public void histogramFractionalWeight() {
        b.histogram("h").addSample(1, 0.5).addSample(4, 2).close();
        b.histogram("i").addSample(1, 0.1).close();
        Decoded d = single();
        assertArrayEquals(new long[] {0x29, 0x39}, d.uints(Column.types));
        assertArrayEquals(new long[] {2, 1}, d.uints(Column.valsUint64));
        assertArrayEquals(new float[] {1, 0.5f, 4, 2}, d.floats(Column.valsFloat32), 0);
        assertArrayEquals(new double[] {1, 0.1}, d.doubles(Column.valsFloat64), 0);
    }

    @Test
    public void set() {
        b.set("s").addMember("").addMember("a").addMember("b").addMember("aa").close();
        Decoded d = single();
        assertArrayEquals(new long[] {0x0a}, d.uints(Column.types));
        assertArrayEquals(new long[] {4}, d.uints(Column.valsUint64));
        assertEquals(Arrays.asList("", "a", "b", "aa"), d.strs(Column.setStrs));
        assertFalse(d.has(Column.valsSint64));
        assertFalse(d.has(Column.valsFloat32));
        assertFalse(d.has(Column.valsFloat64));
    }

    @Test
    public void sketch() {
        Sketch sk = new Sketch();
        sk.build(new long[] {1, 2, 2}, 1.0);

        final List<Long> keys = new ArrayList<>();
        final List<Long> cnts = new ArrayList<>();
        sk.bins(
                new Sketch.BinConsumer() {
                    @Override
                    public void consumeBin(short key, long count) {
                        keys.add((long) key);
                        cnts.add(count);
                    }
                });

        b.sketch("d").setSketch(sk).close();
        Decoded d = single();

        assertArrayEquals(new long[] {0x14}, d.uints(Column.types));
        assertArrayEquals(new long[] {5, 1, 2, 3}, d.sints(Column.valsSint64));
        assertArrayEquals(new long[] {keys.size()}, d.uints(Column.sketchNumBins));
        assertArrayEquals(toArray(keys), d.refs(Column.sketchBinKeys));
        assertArrayEquals(toArray(cnts), d.uints(Column.sketchBinCnts));
    }

    @Test
    public void sketchFloat() {
        Sketch sk = new Sketch();
        sk.build(new double[] {0.5, 1.5}, 1.0);
        b.sketch("d").setSketch(sk).close();
        Decoded d = single();

        ValueType vt =
                PointKind.of(sk.sum())
                        .union(PointKind.of(sk.min()))
                        .union(PointKind.of(sk.max()))
                        .toValueType();
        assertArrayEquals(new long[] {4 | vt.flag()}, d.uints(Column.types));
        assertArrayEquals(new long[] {2}, d.sints(Column.valsSint64));
    }

    @Test
    public void sketchSetTwiceThrows() {
        final Sketch sk = new Sketch();
        sk.build(new long[] {1}, 1.0);
        final AggregationSketch e = b.sketch("d").setSketch(sk);
        assertThrows(
                IllegalStateException.class,
                new ThrowingRunnable() {
                    @Override
                    public void run() {
                        e.setSketch(sk);
                    }
                });
        e.close();
        Decoded d = single();
        assertArrayEquals(new long[] {1}, d.uints(Column.sketchNumBins));
    }

    @Test
    public void mixedValsUint64Interleaving() {
        b.mergeableGauge("a").setValue(1, 7).close();
        b.histogram("b").addSample(1, 1).addSample(2, 1).close();
        b.set("c").addMember("x").close();
        b.count("d").setValue(3).close();
        b.mergeableGauge("e").setValue(4, 9).close();
        Decoded d = single();

        assertEquals(Arrays.asList("a", "b", "c", "d", "e"), d.names());
        assertArrayEquals(new long[] {0x18, 0x19, 0x0a, 0x11, 0x18}, d.uints(Column.types));
        assertArrayEquals(new long[] {7, 2, 1, 9}, d.uints(Column.valsUint64));
        assertArrayEquals(new long[] {1, 1, 1, 2, 1, 3, 4}, d.sints(Column.valsSint64));
        assertEquals(Arrays.asList("x"), d.strs(Column.setStrs));
    }

    @Test
    public void tagsResourcesCardinality() {
        b.count("a")
                .setTagsCardinality(TagsCardinality.LOW)
                .setTags(Arrays.asList("bar", "foo"))
                .setResources(Arrays.asList("host", "h1"))
                .setValue(1)
                .close();
        b.count("a").setTags(Arrays.asList("bar", "foo")).setValue(2).close();
        Decoded d = single();

        assertArrayEquals(new long[] {0x2011, 0x0011}, d.uints(Column.types));
        assertEquals(Arrays.asList("bar", "foo"), d.strs(Column.dictTagStr));
        assertArrayEquals(new long[] {1, 1}, d.refs(Column.tagsetRefs));
        assertArrayEquals(new long[] {1, 0}, d.refs(Column.resourcesRefs));
        assertEquals(Arrays.asList("host", "h1"), d.strs(Column.dictResourceStr));
    }

    @Test
    public void emptyMetricsAreDropped() {
        b.count("a").close();
        b.mergeableGauge("b").close();
        b.histogram("c").close();
        b.set("d").close();
        b.sketch("e").close();
        b.sketch("f").setSketch(new Sketch()).close();
        b.close();
        assertEquals(0, payloads.size());
    }

    @Test
    public void histogramRejectsOutOfOrder() {
        final AggregationHistogram h = b.histogram("h").addSample(1, 1);
        assertRejected(
                new ThrowingRunnable() {
                    @Override
                    public void run() {
                        h.addSample(1, 1);
                    }
                });
        assertRejected(
                new ThrowingRunnable() {
                    @Override
                    public void run() {
                        h.addSample(0.5, 1);
                    }
                });
        assertRejected(
                new ThrowingRunnable() {
                    @Override
                    public void run() {
                        h.addSample(Double.NaN, 1);
                    }
                });
        h.addSample(2, 1).close();
        Decoded d = single();
        assertArrayEquals(new long[] {2}, d.uints(Column.valsUint64));
        assertArrayEquals(new long[] {1, 1, 2, 1}, d.sints(Column.valsSint64));
    }

    @Test
    public void histogramRejectsNaNFirst() {
        final AggregationHistogram h = b.histogram("h");
        assertRejected(
                new ThrowingRunnable() {
                    @Override
                    public void run() {
                        h.addSample(Double.NaN, 1);
                    }
                });
        h.addSample(Double.NEGATIVE_INFINITY, 1).addSample(0, 1).close();
        Decoded d = single();
        assertArrayEquals(new long[] {2}, d.uints(Column.valsUint64));
    }

    @Test
    public void setRejectsOutOfOrder() {
        final AggregationSet s = b.set("s").addMember("bb");
        String[] bad = {
            "bb", // duplicate
            "a", // shorter after longer
            "ba", // same length, smaller
        };
        for (final String m : bad) {
            assertRejected(
                    new ThrowingRunnable() {
                        @Override
                        public void run() {
                            s.addMember(m);
                        }
                    });
        }
        // "é" is one char but two UTF-8 bytes (c3 a9): it is the same length as "bb" and sorts
        // after it bytewise, where a signed compare would order 0xc3 before 'b'. It sorts before
        // "aaa" by length, and a one-byte "z" is rejected after it.
        s.addMember("é");
        assertRejected(
                new ThrowingRunnable() {
                    @Override
                    public void run() {
                        s.addMember("z");
                    }
                });
        s.addMember("aaa").close();
        Decoded d = single();
        assertArrayEquals(new long[] {3}, d.uints(Column.valsUint64));
        assertEquals(Arrays.asList("bb", "é", "aaa"), d.strs(Column.setStrs));
    }

    @Test
    public void compareShortlex() {
        assertTrue(AggregationSet.compareShortlex(bytes("b"), bytes("aa")) < 0);
        assertTrue(AggregationSet.compareShortlex(bytes("aa"), bytes("ab")) < 0);
        assertTrue(AggregationSet.compareShortlex(bytes("bb"), bytes("é")) < 0);
        assertTrue(AggregationSet.compareShortlex(bytes("é"), bytes("bb")) > 0);
        assertEquals(0, AggregationSet.compareShortlex(bytes("ab"), bytes("ab")));
        assertTrue(AggregationSet.compareShortlex(bytes(""), bytes("a")) < 0);
    }

    @Test
    public void resetAfterThrow() {
        final AggregationHistogram h = b.histogram("h");
        h.setResources(Arrays.asList("host", "x"));
        assertThrows(
                IllegalArgumentException.class,
                new ThrowingRunnable() {
                    @Override
                    public void run() {
                        b.count("bad").setResources(Arrays.asList("odd"));
                    }
                });
        b.resetMetric();
        b.count("ok").setValue(1).close();
        Decoded d = single();
        assertEquals(Arrays.asList("ok"), d.names());
    }

    @Test
    public void splitAtSizeLimit() {
        char[] pad = new char[1000];
        Arrays.fill(pad, 'x');
        String padding = new String(pad);

        int numMetrics = 600;
        for (int i = 0; i < numMetrics; i++) {
            AggregationSet s =
                    b.set("name" + (i % 3))
                            .setTags(Arrays.asList("tag:" + (i % 5)))
                            .setResources(Arrays.asList("host", "h" + (i % 2)));
            s.addMember(String.format("%04d", i)).addMember(padding);
            s.close();
        }
        b.close();

        assertTrue(payloads.size() > 1);
        int seen = 0;
        for (byte[] p : payloads) {
            assertTrue(p.length <= 256 * 1024 + 4);
            Decoded d = new Decoded(p);
            d.assertNoPerPointColumns();

            List<String> names = d.names();
            List<String> tags = d.strs(Column.dictTagStr);
            long[] tagsetRefs = d.refs(Column.tagsetRefs);
            long[] tagsets = d.sints(Column.dictTagsets);
            List<String> members = d.strs(Column.setStrs);
            long[] counts = d.uints(Column.valsUint64);
            assertEquals(names.size(), counts.length);

            for (int j = 0; j < names.size(); j++, seen++) {
                assertEquals("name" + (seen % 3), names.get(j));
                // Each tagset has one tag: [length=1, tagIndex].
                int tagset = (int) tagsetRefs[j];
                assertEquals(1, tagsets[2 * (tagset - 1)]);
                int tag = (int) tagsets[2 * (tagset - 1) + 1];
                assertEquals("tag:" + (seen % 5), tags.get(tag - 1));
                assertEquals(2, counts[j]);
                assertEquals(String.format("%04d", seen), members.get(2 * j));
                assertEquals(padding, members.get(2 * j + 1));
            }
        }
        assertEquals(numMetrics, seen);
    }

    static void assertRejected(ThrowingRunnable r) {
        assertThrows(IllegalArgumentException.class, r);
    }

    static byte[] bytes(String s) {
        return s.getBytes(UTF_8);
    }

    static long[] toArray(List<Long> l) {
        long[] r = new long[l.size()];
        for (int i = 0; i < r.length; i++) r[i] = l.get(i);
        return r;
    }
}
