/* Unless explicitly stated otherwise all files in this repository are
 * licensed under the Apache 2.0 License.
 *
 * This product includes software developed at Datadog
 *  (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.
 */

package com.datadoghq.dogstatsd.http.serializer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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

        b.sketch("ijk")
                .setTags(Arrays.asList(new String[] {"foo", "baz"}))
                .addPoint(100, 4.75, 1.25, 1.75, 3, new int[] {1351, 1373}, new int[] {1, 2})
                .addPoint(110, 6.5, 2.25, 2.75, 5, new int[] {1389, 1402}, new int[] {2, 3})
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
                    188,
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
                    0x24,
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
                    4,
                    2,
                    4,
                    6,
                    10,
                    // valsFloat32, list(pack('<ffffff', 4.75, 1.25, 1.75, 6.5, 2.25, 2.75))
                    (18 << 3) | 2,
                    1,
                    24,
                    0,
                    0,
                    152,
                    64,
                    0,
                    0,
                    160,
                    63,
                    0,
                    0,
                    224,
                    63,
                    0,
                    0,
                    208,
                    64,
                    0,
                    0,
                    16,
                    64,
                    0,
                    0,
                    48,
                    64,
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
                    142,
                    21,
                    44,
                    218,
                    21,
                    26,
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
