/* Unless explicitly stated otherwise all files in this repository are
 * licensed under the Apache 2.0 License.
 *
 * This product includes software developed at Datadog
 *  (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.
 */

package com.datadoghq.dogstatsd;

import java.util.Arrays;

/**
 * Reusable DDSketch builder. Consumes a batch of observations and populates sum, min, max, count
 * and distribution bins accordingly.
 *
 * <p>This implementation maintains at most 4096 bins with 64-bit counters. Number of bins is a hard
 * limit and is enforced by the intake.
 *
 * <p>Prioritizes accuracy of higher key bins (higher percentiles) over lower ones when number of
 * bins exceeds the limit.
 */
public class Sketch {
    static final double gamma = 130.0 / 128;
    static final double minValue = 1e-9;
    static final double logGamma = Math.log(gamma);
    static final int bias = 1 - (int) (Math.floor(Math.log(minValue) / logGamma));
    static final short posInfKey = (1 << 15) - 1;
    static final short negInfKey = -posInfKey;
    static final int binLimit = 4096;

    // Growable ring buffer up to binLimit elements.
    short[] binKeys = new short[0];
    long[] binCounts = new long[0];
    int size;
    int head;

    double min;
    double max;
    double sum;
    long count;

    static short key(double value) {
        if (value < 0) {
            return (short) -key(-value);
        }

        if (value < minValue) {
            return 0;
        }

        int key = (int) Math.rint(Math.log(value) / logGamma) + bias;
        if (key > posInfKey) {
            return posInfKey;
        }
        return (short) key;
    }

    /** Receives (key, count) pairs from {@link #bins(BinConsumer)}. */
    public interface BinConsumer {
        void consumeBin(short key, long count);
    }

    /**
     * @return the number of populated bins fed to {@link #bins(BinConsumer)}.
     */
    public int size() {
        return size;
    }

    /** Feeds each populated bin to {@code consumer} in order. */
    public void bins(BinConsumer consumer) {
        int idx = head;
        for (int i = 0; i < size; i++) {
            consumer.consumeBin(binKeys[idx], binCounts[idx]);
            idx++;
            if (idx == binKeys.length) {
                idx = 0;
            }
        }
    }

    /**
     * @return the minimum observation in the most recent batch, or {@code 0} if the batch was
     *     empty.
     */
    public double min() {
        return min;
    }

    /**
     * @return the maximum observation in the most recent batch, or {@code 0} if the batch was
     *     empty.
     */
    public double max() {
        return max;
    }

    /**
     * @return the sum of observations in the most recent batch.
     */
    public double sum() {
        return sum;
    }

    /**
     * @return the total count of observations represented by the sketch.
     */
    public long count() {
        return count;
    }

    /**
     * Builds the sketch from the given values. The {@code values} array is modified in place
     * (sorted); callers that need to preserve the original ordering should pass a copy.
     *
     * @param values the observations to include in the sketch; sorted in place
     * @param sampleRate the sampling rate used to collect {@code values}, in {@code (0, 1]}. Each
     *     observation is weighted by {@code 1 / sampleRate} when accumulating counts and sums.
     *     Rates below ~1.08e-19 saturate the per-observation weight; bin counts and the total
     *     {@code count} field saturate at {@link Long#MAX_VALUE} on overflow.
     */
    public void build(long[] values, double sampleRate) {
        if (Double.isNaN(sampleRate) || sampleRate <= 0 || sampleRate > 1) {
            throw new IllegalArgumentException("sampleRate is out of range");
        }

        reset();
        buildInner(values, sampleRate);
    }

    private void buildInner(final long[] values, double sampleRate) {
        if (values == null || values.length == 0) {
            return;
        }

        Arrays.sort(values);

        final long sampleSize = (long) (1 / sampleRate);
        min = values[0];
        max = values[values.length - 1];
        count = satMul(sampleSize, values.length);

        short topKey = negInfKey - 1;
        long topCount = 0;

        for (long val : values) {
            sum += val / sampleRate;

            short key = key(val);

            if (key == topKey) {
                topCount = satAdd(topCount, sampleSize);
            } else {
                if (topCount > 0) {
                    append(topKey, topCount);
                }
                topKey = key;
                topCount = sampleSize;
            }
        }

        append(topKey, topCount);
    }

    private void reset() {
        min = 0;
        max = 0;
        sum = 0;
        count = 0;
        size = 0;
        head = 0;
    }

    private void append(short key, long count) {
        if (size >= binLimit) {
            int next = head + 1;
            if (next == binLimit) {
                next = 0;
            }
            binCounts[next] = satAdd(binCounts[next], binCounts[head]);
            binKeys[head] = key;
            binCounts[head] = count;
            head = next;
            return;
        }

        if (size == binKeys.length) {
            int cap = Math.max(4, size * 2);
            binKeys = Arrays.copyOf(binKeys, cap);
            binCounts = Arrays.copyOf(binCounts, cap);
        }
        binKeys[size] = key;
        binCounts[size] = count;
        size++;
    }

    // a >= 0 && b >= 0
    private static long satAdd(long a, long b) {
        long r = a + b;
        if (r < 0) {
            r = Long.MAX_VALUE;
        }
        return r;
    }

    // a >= 0 && b >= 0
    private static long satMul(long a, long b) {
        long r = a * b;
        if ((a > Integer.MAX_VALUE || b > Integer.MAX_VALUE) && a != 0 && r / a != b) {
            r = Long.MAX_VALUE;
        }
        return r;
    }
}
