/* Unless explicitly stated otherwise all files in this repository are
 * licensed under the Apache 2.0 License.
 *
 * This product includes software developed at Datadog
 *  (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.
 */

package com.datadoghq.dogstatsd.http.serializer;

import java.nio.BufferOverflowException;

abstract class Buffer {
    protected int size;

    // more must be >= 0.
    // if this returns, size + more <= INT_MAX && size + more <= data.length
    protected final void reserve(int more) {
        // size + more > capacity, but without integer overflow
        if (size > capacity() - more) {
            grow(more);
        }
    }

    protected final void grow(int more) {
        if (size > Integer.MAX_VALUE - more) {
            throw new BufferOverflowException();
        }
        int newSize = size + more;
        int cap = capacity();
        if (cap < Integer.MAX_VALUE / 2 && newSize < cap * 2) {
            newSize = cap * 2;
        }
        realloc(newSize);
    }

    int length() {
        return size;
    }

    void clear() {
        size = 0;
    }

    /** Return true if buf is null or empty */
    static boolean isEmpty(Buffer buf) {
        return buf == null || buf.size == 0;
    }

    abstract int capacity();

    protected abstract void realloc(int newSize);
}
