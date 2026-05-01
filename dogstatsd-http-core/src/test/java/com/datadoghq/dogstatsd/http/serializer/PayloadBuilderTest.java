/* Unless explicitly stated otherwise all files in this repository are
 * licensed under the Apache 2.0 License.
 *
 * This product includes software developed at Datadog
 *  (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.
 */

package com.datadoghq.dogstatsd.http.serializer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.datadoghq.dogstatsd.Sketch;
import java.util.ArrayList;
import java.util.Arrays;
import org.junit.Test;

public class PayloadBuilderTest {
    @Test
    public void simple() {
        final ArrayList<byte[]> payloads = new ArrayList<>();
        PayloadBuilder b =
                new PayloadBuilder(
                        new PayloadConsumer() {
                            @Override
                            public void handle(byte[] p) {
                                payloads.add(p);
                            }
                        });

        b.count("unused")
                .setTags(Arrays.asList(new String[] {"foo", "bar"}))
                .addPoint(100, 1)
                .close();
        b.flushPayload();

        b.count("abc")
                .setTags(Arrays.asList(new String[] {"foo", "bar"}))
                .setResources(Arrays.asList(new String[] {"host", ""}))
                .addPoint(100, 1)
                .addPoint(110, 2)
                .close();

        b.gauge("defgh").addPoint(100, 0).close();

        Sketch sketch1 = new Sketch();
        sketch1.build(new long[] {1, 2, 2}, 1.0);
        Sketch sketch2 = new Sketch();
        sketch2.build(new long[] {2, 2, 3, 3, 3}, 5e-10);

        b.sketch("ijk")
                .setTags(Arrays.asList(new String[] {"foo", "baz"}))
                .addPoint(100, sketch1)
                .addPoint(110, sketch2)
                .close();

        b.rate("lm").setInterval(10).addPoint(100, 3.14).close();
        b.rate("no").addPoint(100, 1).addPoint(110, 1.5).close();
        b.rate("pq").addPoint(100, 1L << 25).addPoint(110, 1.5).close();

        b.close();

        assertEquals(2, payloads.size());
        byte[] p = payloads.get(1);

        TestUtil.assertPayload(
                p,
                new int[] {
                    // MetricData
                    (3 << 3) | 2,
                    243,
                    1,
                    // dictNameStr
                    (1 << 3) | 2,
                    23,
                    3,
                    97,
                    98,
                    99, // abc
                    5,
                    100,
                    101,
                    102,
                    103,
                    104, // defgh
                    3,
                    105,
                    106,
                    107, // ijk
                    2,
                    108,
                    109, // lm
                    2,
                    110,
                    111, // no
                    2,
                    112,
                    113, // pq
                    // dictTagsStr
                    (2 << 3) | 2,
                    12,
                    3,
                    98,
                    97,
                    114, // bar
                    3,
                    102,
                    111,
                    111, // foo
                    3,
                    98,
                    97,
                    122, // baz
                    // dictTagsets
                    (3 << 3) | 2,
                    6,
                    4,
                    2,
                    2,
                    4,
                    4,
                    2,
                    // dictResourcesStr
                    (4 << 3) | 2,
                    5,
                    4,
                    104,
                    111,
                    115,
                    116, // host
                    // dictResourcesLen
                    (5 << 3) | 2,
                    1,
                    1,
                    // dictResourcesType
                    (6 << 3) | 2,
                    1,
                    2,
                    // dictResourcesName
                    (7 << 3) | 2,
                    1,
                    0,
                    // dictSourceTypeName is empty
                    // dictOrigin
                    (9 << 3) | 2,
                    3,
                    10,
                    10,
                    0,
                    // types
                    (10 << 3) | 2,
                    6,
                    0x11,
                    0x03,
                    0x14,
                    0x32,
                    0x22,
                    0x32,
                    // names
                    (11 << 3) | 2,
                    6,
                    2,
                    2,
                    2,
                    2,
                    2,
                    2,
                    // tags
                    (12 << 3) | 2,
                    6,
                    2,
                    1,
                    4,
                    3,
                    0,
                    0,
                    // resources
                    (13 << 3) | 2,
                    6,
                    2,
                    1,
                    0,
                    0,
                    0,
                    0,
                    // intervals
                    (14 << 3) | 2,
                    6,
                    0,
                    0,
                    0,
                    10,
                    0,
                    0,
                    // numPoints
                    (15 << 3) | 2,
                    6,
                    2,
                    1,
                    2,
                    1,
                    2,
                    2,
                    // timestamps
                    (16 << 3) | 2,
                    1,
                    11,
                    200,
                    1,
                    20,
                    19,
                    0,
                    20,
                    19,
                    0,
                    20,
                    19,
                    20,
                    // valsSint64
                    (17 << 3) | 2,
                    1,
                    19,
                    2,
                    4,
                    10,
                    2,
                    4,
                    6,
                    128,
                    144,
                    196,
                    219,
                    193,
                    1,
                    4,
                    6,
                    246,
                    143,
                    223,
                    192,
                    74,
                    // valsFloat32, list(pack('<ff', 1, 1.5))
                    (18 << 3) | 2,
                    1,
                    8,
                    0,
                    0,
                    128,
                    63,
                    0,
                    0,
                    192,
                    63,
                    // valsFloat64, list(pack('<ddd', 3.14, 1<<25, 1.5))
                    (19 << 3) | 2,
                    1,
                    24,
                    31,
                    133,
                    235,
                    81,
                    184,
                    30,
                    9,
                    64,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    128,
                    65,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    248,
                    63,
                    // sketchNumBins
                    (20 << 3) | 2,
                    1,
                    2,
                    2,
                    2,
                    // sketchBinKeys
                    (21 << 3) | 2,
                    1,
                    7,
                    244,
                    20,
                    90,
                    206,
                    21,
                    52,
                    0,
                    // sketchBinCnts
                    (22 << 3) | 2,
                    1,
                    17,
                    1,
                    2,
                    254,
                    207,
                    172,
                    243,
                    14,
                    255,
                    255,
                    255,
                    255,
                    15,
                    254,
                    247,
                    130,
                    173,
                    6,
                    // sourceTypeName
                    (23 << 3) | 2,
                    1,
                    6,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    // origins
                    (24 << 3) | 2,
                    1,
                    6,
                    2,
                    0,
                    0,
                    0,
                    0,
                    0,
                });
    }

    @Test
    public void split() {
        final ArrayList<byte[]> payloads = new ArrayList<>();
        PayloadBuilder b =
                new PayloadBuilder(
                        new PayloadConsumer() {
                            @Override
                            public void handle(byte[] p) {
                                payloads.add(p);
                            }
                        });

        for (int i = 0; i < 1000; i++) {
            ScalarMetric g = b.gauge(String.format("custom.metric.%d", i));
            g.setTags(Arrays.asList(new String[] {String.format("foobar.%d", i)}));
            for (int j = 0; j < 100; j++) {
                g.addPoint(100 + j * 10, (double) j);
            }
            g.close();
        }
        b.close();

        assertEquals(2, payloads.size());

        for (byte[] p : payloads) {
            assertTrue(p.length <= b.maxPayloadSize);
        }
    }
}
