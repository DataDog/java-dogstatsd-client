/* Unless explicitly stated otherwise all files in this repository are
 * licensed under the Apache 2.0 License.
 *
 * This product includes software developed at Datadog
 *  (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.
 */

package com.datadoghq.dogstatsd.http.serializer;

class DeltaEncoder {
    private long prev = 0;

    long encode(long v) {
        long r = v - prev;
        prev = v;
        return r;
    }

    void clear() {
        prev = 0;
    }
}
