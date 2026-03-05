/* Unless explicitly stated otherwise all files in this repository are
 * licensed under the Apache 2.0 License.
 *
 * This product includes software developed at Datadog
 *  (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.
 */

package com.datadoghq.dogstatsd.http.serializer;

enum ValueType {
    zero(0),
    sint64(0x10),
    float32(0x20),
    float64(0x30);

    private final int flag;

    ValueType(int flag) {
        this.flag = flag;
    }

    int flag() {
        return flag;
    }
}
