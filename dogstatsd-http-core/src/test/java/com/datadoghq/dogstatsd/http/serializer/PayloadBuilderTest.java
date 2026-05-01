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
        sketch2.build(new long[] {2, 2, 3, 3, 3}, 1.0);

        b.sketch("ijk")
                .setTags(Arrays.asList(new String[] {"foo", "baz"}))
                .addPoint(100, sketch1)
                .addPoint(110, sketch2)
                .close();

        b.rate("lm").setInterval(10).addPoint(100, 3.14).close();

        b.close();

        assertEquals(2, payloads.size());
        byte[] p = payloads.get(1);

        TestUtil.assertPayload(
                p,
                new int[] {
                    // MetricData
                    (3 << 3) | 2,
                    167,
                    1,
                    // dictNameStr
                    (1 << 3) | 2,
                    17,
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
                    4,
                    0x11,
                    0x03,
                    0x14,
                    0x32,
                    // names
                    (11 << 3) | 2,
                    4,
                    2,
                    2,
                    2,
                    2,
                    // tags
                    (12 << 3) | 2,
                    4,
                    2,
                    1,
                    4,
                    3,
                    // resources
                    (13 << 3) | 2,
                    4,
                    2,
                    1,
                    0,
                    0,
                    // intervals
                    (14 << 3) | 2,
                    4,
                    0,
                    0,
                    0,
                    10,
                    // numPoints
                    (15 << 3) | 2,
                    4,
                    2,
                    1,
                    2,
                    1,
                    // timestamps
                    (16 << 3) | 2,
                    1,
                    7,
                    200,
                    1,
                    20,
                    19,
                    0,
                    20,
                    19,
                    // valsSint64
                    (17 << 3) | 2,
                    1,
                    10,
                    2,
                    4,
                    10,
                    2,
                    4,
                    6,
                    26,
                    4,
                    6,
                    10,
                    // valsFloat64, list(pack('<d', 3.14))
                    (19 << 3) | 2,
                    1,
                    8,
                    31,
                    133,
                    235,
                    81,
                    184,
                    30,
                    9,
                    64,
                    // sketchNumBins
                    (20 << 3) | 2,
                    1,
                    2,
                    2,
                    2,
                    // sketchBinKeys
                    (21 << 3) | 2,
                    1,
                    6,
                    244,
                    20,
                    90,
                    206,
                    21,
                    52,
                    // sketchBinCnts
                    (22 << 3) | 2,
                    1,
                    4,
                    1,
                    2,
                    2,
                    3,
                    // sourceTypeName
                    (23 << 3) | 2,
                    1,
                    4,
                    0,
                    0,
                    0,
                    0,
                    // origins
                    (24 << 3) | 2,
                    1,
                    4,
                    2,
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
