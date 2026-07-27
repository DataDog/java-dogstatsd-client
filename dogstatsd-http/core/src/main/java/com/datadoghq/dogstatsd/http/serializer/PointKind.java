/* Unless explicitly stated otherwise all files in this repository are
 * licensed under the Apache 2.0 License.
 *
 * This product includes software developed at Datadog
 *  (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.
 */

package com.datadoghq.dogstatsd.http.serializer;

enum PointKind {
    zero(0),
    int24(1),
    float32(2),
    int48(3),
    float64(4);

    private final int rank;

    PointKind(int rank) {
        this.rank = rank;
    }

    static PointKind of(double v) {
        if (v == 0) {
            return PointKind.zero;
        }

        // Integers in this range can still fit into float32 column if needed.
        final long maxInt24 = 1L << 24;
        final long minInt24 = -1L << 24;
        // Integers in this range encode to 7 byte varints or less.
        final long maxInt48 = 1L << 48;
        final long minInt48 = -1L << 48;
        final long i = (long) v;
        if ((double) i == v) {
            if (i >= minInt24 && i <= maxInt24) {
                return PointKind.int24;
            }
            if (i >= minInt48 && i <= maxInt48) {
                return PointKind.int48;
            }
        }
        final float f = (float) v;
        if ((double) f == v) {
            return PointKind.float32;
        }
        return PointKind.float64;
    }

    static PointKind of(DoubleBuffer values) {
        PointKind kind = PointKind.zero;
        for (int i = 0; i < values.length(); i++) {
            kind = kind.union(of(values.get(i)));
        }
        return kind;
    }

    PointKind union(PointKind o) {
        if ((this == int48 && o == float32) || (this == float32 && o == int48)) {
            return PointKind.float64;
        }
        if (o.rank > rank) {
            return o;
        }
        return this;
    }

    ValueType toValueType() {
        switch (this) {
            case zero:
                return ValueType.zero;
            case int24:
            case int48:
                return ValueType.sint64;
            case float32:
                return ValueType.float32;
            default:
                return ValueType.float64;
        }
    }
}
