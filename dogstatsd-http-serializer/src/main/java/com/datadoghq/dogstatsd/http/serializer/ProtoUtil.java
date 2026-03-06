/* Unless explicitly stated otherwise all files in this repository are
 * licensed under the Apache 2.0 License.
 *
 * This product includes software developed at Datadog
 *  (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.
 */

package com.datadoghq.dogstatsd.http.serializer;

class ProtoUtil {
    static final int ID_SHIFT = 3;
    static final int TYPE_BYTES = 2;

    static int varintLen(long v) {
        if (v == 0) {
            return 1;
        }
        int n = 64 - Long.numberOfLeadingZeros(v);
        return (n + 6) / 7;
    }

    static int fieldLen(int id, int len) {
        return varintLen(id << ID_SHIFT) + varintLen(len) + len;
    }

    static long bytesFieldHeader(int id) {
        return (id << ID_SHIFT) | TYPE_BYTES;
    }
}
