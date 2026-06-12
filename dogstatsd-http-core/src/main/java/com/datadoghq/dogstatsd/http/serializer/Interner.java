/* Unless explicitly stated otherwise all files in this repository are
 * licensed under the Apache 2.0 License.
 *
 * This product includes software developed at Datadog
 *  (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.
 */

package com.datadoghq.dogstatsd.http.serializer;

import java.util.HashMap;

class Interner<T> {
    interface Encoder<T> {
        void encode(T val);
    }

    private HashMap<T, Long> inner = new HashMap<>();
    private long lastId = 0;
    private final Encoder<T> encoder;
    private final T empty;

    Interner(T empty, Encoder<T> encoder) {
        this.empty = empty;
        this.encoder = encoder;
        clear();
    }

    long intern(T val) {
        if (val == null || empty.equals(val)) {
            return 0;
        }

        Long id = inner.get(val);
        if (id != null) {
            return id;
        }

        encoder.encode(val);

        lastId++;
        inner.put(val, lastId);
        return lastId;
    }

    void clear() {
        inner.clear();
        lastId = 0;
    }
}
