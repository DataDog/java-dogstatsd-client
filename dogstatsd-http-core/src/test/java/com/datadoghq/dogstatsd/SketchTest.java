/* Unless explicitly stated otherwise all files in this repository are
 * licensed under the Apache 2.0 License.
 *
 * This product includes software developed at Datadog
 *  (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.
 */

package com.datadoghq.dogstatsd;

import static com.datadoghq.dogstatsd.Sketch.key;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import org.junit.Test;
import org.junit.function.ThrowingRunnable;

public class SketchTest {
    Sketch s = new Sketch();

    void assertBins(final short[] expectedKeys, final long[] expectedCounts) {
        assertEquals(expectedKeys.length, expectedCounts.length);
        assertEquals(expectedKeys.length, s.size());
        final int[] i = {0};
        s.bins(
                new Sketch.BinConsumer() {
                    @Override
                    public void consumeBin(short key, long count) {
                        assertEquals(expectedKeys[i[0]], key);
                        assertEquals(expectedCounts[i[0]], count);
                        i[0]++;
                    }
                });
        assertEquals(i[0], s.size());
    }

    @Test
    public void keys() {
        final double oneSmaller = Double.longBitsToDouble(Double.doubleToLongBits(1e-9) - 1);
        assertEquals(32767, key(1e300));
        assertEquals(Sketch.bias, key(1));
        assertEquals(0, key(oneSmaller));
        assertEquals(0, key(0));
        assertEquals(0, key(-oneSmaller));
        assertEquals(-Sketch.bias, key(-1));
        assertEquals(-32767, key(-1e300));
    }

    @Test
    public void basic() {
        s.build(null, 1);
        assertEquals(0, s.min(), 0);
        assertEquals(0, s.max(), 0);
        assertEquals(0, s.sum(), 0);
        assertEquals(0, s.count(), 0);
        assertBins(new short[] {}, new long[] {});

        s.build(new long[] {423, 234}, 0.25);
        assertEquals(234, s.min(), 0);
        assertEquals(423, s.max(), 0);
        assertEquals(2628, s.sum(), 0);
        assertEquals(8, s.count(), 0);
        assertBins(new short[] {1690, 1728}, new long[] {4, 4});

        s.build(new long[] {}, 1);
        assertEquals(0, s.min(), 0);
        assertEquals(0, s.max(), 0);
        assertEquals(0, s.sum(), 0);
        assertEquals(0, s.count(), 0);
        assertBins(new short[] {}, new long[] {});

        final long[] values = new long[] {4, 2, 3, 1, 0, 3, -1, -2};
        assertThrows(
                IllegalArgumentException.class,
                new ThrowingRunnable() {
                    @Override
                    public void run() {
                        s.build(values, 2);
                    }
                });
    }

    @Test
    public void subIntSampleRate() {
        s.build(new long[] {1, 1}, 1e-15);
        long expected = 2L * (long) (1 / 1e-15);
        assertEquals(expected, s.count());
        assertBins(new short[] {1338}, new long[] {expected});
    }

    @Test
    public void saturatingCount() {
        long[] values = new long[16];
        java.util.Arrays.fill(values, 1);
        s.build(values, 1e-18);
        assertEquals(Long.MAX_VALUE, s.count());
        assertBins(new short[] {1338}, new long[] {Long.MAX_VALUE});
    }

    @Test
    public void saturatingExtremeRate() {
        s.build(new long[] {1}, Double.MIN_VALUE);
        assertEquals(Long.MAX_VALUE, s.count());
        assertBins(new short[] {1338}, new long[] {Long.MAX_VALUE});
    }

    @Test
    public void binMerge() {
        long[] values = new long[Sketch.binLimit + 4];
        long val = 1;
        for (int i = 0; i < values.length / 2; i++) {
            val = Math.max(val + 1, (long) Math.ceil(val * Sketch.gamma));
            values[i] = val;
            values[i + values.length / 2] = -val;
        }
        s.build(values, 1);
        assertEquals(values[0], s.min(), 0);
        assertEquals(values[values.length - 1], s.max(), 0);

        final short foldedKey = Sketch.key(values[4]);

        s.bins(
                new Sketch.BinConsumer() {
                    int i;

                    @Override
                    public void consumeBin(short key, long count) {
                        if (i == 0) {
                            assertEquals(foldedKey, key);
                            assertEquals(5, count);
                        } else {
                            assertEquals(1, count);
                        }
                        i++;
                    }
                });
    }

    @Test
    public void saturatingBinMerge() {
        long[] values = new long[Sketch.binLimit + 2];
        long val = 1;
        for (int i = 0; i < values.length / 2; i++) {
            val = Math.max(val + 1, (long) Math.ceil(val * Sketch.gamma));
            values[i] = val;
            values[i + values.length / 2] = -val;
        }

        final long expectedCount = (long) 5.0e18;

        s.build(values, 1.0 / expectedCount);
        s.bins(
                new Sketch.BinConsumer() {
                    int i;

                    @Override
                    public void consumeBin(short key, long count) {
                        if (i == 0) {
                            assertEquals(Long.MAX_VALUE, count);
                        } else {
                            assertEquals(expectedCount, count);
                        }
                        i++;
                    }
                });
    }
}
