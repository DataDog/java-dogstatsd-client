package com.timgroup.statsd;

import java.util.Arrays;

class DirectSketch {
    static final double gamma = 130.0 / 128;
    static final double minValue = 1e-9;
    static final double logGamma = Math.log(gamma);
    static final int bias = 1 - (int)(Math.floor(Math.log(minValue) / logGamma));
    static final int posInfKey = (1 << 15) - 1;
    static final int negInfKey = -posInfKey;
    static final int maxCount = Integer.MAX_VALUE;

    ProtobufWriter keys = new ProtobufWriter();
    ProtobufWriter bins = new ProtobufWriter();

    double min;
    double max;
    double sum;
    double cnt;

    int topKey;
    int topCount;

    static int key(double value) {
        if (value < 0) {
            return -key(-value);
        }

        if (value < minValue) {
            return 0;
        }

        int key = (int)Math.rint(Math.log(value) / logGamma) + bias;
        if (key > posInfKey) {
            return posInfKey;
        }
        return key;
    }

    void reset() {
        min = 0;
        max = 0;
        sum = 0;
        cnt = 0;
        keys.clear();
        bins.clear();

        topKey = negInfKey - 1;
        topCount = 0;
    }

    void append(int key, int count) {
        keys.bareLong(key);
        bins.bareVarint(count);
    }

    void build(final long[] values, final double sampleRate) {
        reset();
        buildInner(values, sampleRate);
        keys.flip();
        bins.flip();
    }

    private void buildInner(final long[] values, double sampleRate) {
        if (values == null || values.length == 0) {
            return;
        }

        Arrays.sort(values);

        if (Double.isNaN(sampleRate) || sampleRate <= 0 || sampleRate > 1) {
            sampleRate = 1;
        }

        final double sampleSize = 1 / sampleRate;
        min = values[0];
        max = values[0];
        cnt = sampleSize * (double)values.length;

        for (long val : values) {
            min = Math.min(min, val);
            max = Math.max(max, val);
            sum += val * sampleSize;

            int key = key(val);

            if (key == topKey) {
                int remain = (int)sampleSize;
                while (topCount > maxCount - remain) {
                    remain -= maxCount - topCount;
                    append(key, maxCount);
                    topCount = 0;
                }
                topCount += remain;
            } else {
                if (topCount > 0) {
                    append(topKey, topCount);
                }
                topKey = key;
                topCount = (int)sampleSize;
            }
        }

        append(topKey, topCount);
    }

    void serialize(ProtobufWriter pw, long timestamp) {
        pw.fieldVarint(1, timestamp);
        pw.fieldVarint(2, (long)cnt);
        pw.fieldDouble(3, min);
        pw.fieldDouble(4, max);
        pw.fieldDouble(5, sum / cnt);
        pw.fieldDouble(6, sum);
        pw.fieldPacked(7, keys);
        pw.fieldPacked(8, bins);
    }
}
