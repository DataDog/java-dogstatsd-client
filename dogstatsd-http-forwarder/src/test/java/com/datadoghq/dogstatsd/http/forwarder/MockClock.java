/* Unless explicitly stated otherwise all files in this repository are
 * licensed under the Apache 2.0 License.
 *
 * This product includes software developed at Datadog
 *  (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.
 */

package com.datadoghq.dogstatsd.http.forwarder;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

/** Manually-advanced wall clock for deterministic interval-start tests. */
final class MockClock extends Clock {
    private Instant instant;

    MockClock(Instant initial) {
        this.instant = initial;
    }

    @Override
    public ZoneId getZone() {
        return ZoneId.of("UTC");
    }

    @Override
    public Clock withZone(ZoneId zone) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Instant instant() {
        return instant;
    }

    void advanceMillis(long millis) {
        instant = instant.plusMillis(millis);
    }
}
