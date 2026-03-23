/* Unless explicitly stated otherwise all files in this repository are
 * licensed under the Apache 2.0 License.
 *
 * This product includes software developed at Datadog
 *  (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.
 */

package com.datadoghq.dogstatsd.http.serializer;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class PointKindTest {
    @Test
    public void simple() {
        assertEquals(ValueType.zero, typeOf(0, -0));
        assertEquals(ValueType.sint64, typeOf(-10, 0, 10));
        assertEquals(ValueType.sint64, typeOf(-1 << 32, 0, 1 << 32));
        assertEquals(ValueType.float32, typeOf(-10, -0.5, 0, 1.25, 10));
        assertEquals(ValueType.float64, typeOf(3.14159, 0));
        // Large integer values should not be represented as float32 to avoid truncation.
        assertEquals(ValueType.float64, typeOf(-1L << 30, 1.5));
        assertEquals(ValueType.float64, typeOf(1.5, -1L << 30));
    }

    static ValueType typeOf(double... values) {
        DoubleBuffer buf = new DoubleBuffer();
        for (double v : values) {
            buf.put(v);
        }
        return PointKind.of(buf).toValueType();
    }
}
