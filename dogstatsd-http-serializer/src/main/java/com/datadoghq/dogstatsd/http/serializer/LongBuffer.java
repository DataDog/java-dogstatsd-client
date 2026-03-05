/* Unless explicitly stated otherwise all files in this repository are
 * licensed under the Apache 2.0 License.
 *
 * This product includes software developed at Datadog
 *  (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.
 */

package com.datadoghq.dogstatsd.http.serializer;

import java.util.Arrays;

class LongBuffer extends Buffer {
    long[] data = new long[16];

    void put(long v) {
        reserve(1);
        data[size++] = v;
    }

    long get(int i) {
        return data[i];
    }

    @Override
    int capacity() {
        return data.length;
    }

    @Override
    protected void realloc(int newSize) {
        data = Arrays.copyOf(data, newSize);
    }

    void sort() {
        Arrays.sort(data, 0, size);
    }
}
