/* Unless explicitly stated otherwise all files in this repository are
 * licensed under the Apache 2.0 License.
 *
 * This product includes software developed at Datadog
 *  (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.
 */

package com.datadoghq.dogstatsd.http.forwarder;

import java.util.function.LongSupplier;

/** Manually-advanced nanoTime source for deterministic age tests. */
final class MockNanos implements LongSupplier {
    private long nanos;

    MockNanos(long initial) {
        this.nanos = initial;
    }

    void advanceMillis(long millis) {
        nanos += millis * 1_000_000L;
    }

    @Override
    public long getAsLong() {
        return nanos;
    }
}
