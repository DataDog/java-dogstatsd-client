/* Unless explicitly stated otherwise all files in this repository are
 * licensed under the Apache 2.0 License.
 *
 * This product includes software developed at Datadog
 *  (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.
 */

package com.datadoghq.dogstatsd.http.forwarder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.time.Clock;
import java.time.Instant;
import org.junit.Test;

public class TelemetryTest {

    private static void assertCounters(
            long expectedPayloads, long expectedBytes, Telemetry.Snapshot.CodeCounters c) {
        assertNotNull(c);
        assertEquals(expectedPayloads, c.payloads);
        assertEquals(expectedBytes, c.bytes);
    }

    @Test
    public void freshState() {
        Telemetry t = new Telemetry();
        Telemetry.Snapshot s = t.snapshot(null);
        assertEquals(0, s.enqueuedBytes);
        assertEquals(0, s.deliveredBytes);
        assertEquals(0, s.enqueuedPayloads);
        assertEquals(0, s.deliveredPayloads);
        assertEquals(0L, s.lastSuccessAgeNanos);
        assertEquals(0L, s.oldestEnqueuedAgeNanos);
        assertTrue(s.byCode.isEmpty());
    }

    @Test
    public void enqueue() {
        Telemetry t = new Telemetry();
        t.onEnqueue(10);
        t.onEnqueue(25);
        t.onEnqueue(5);
        Telemetry.Snapshot s = t.snapshot(null);
        assertEquals(3, s.enqueuedPayloads);
        assertEquals(40, s.enqueuedBytes);
        assertEquals(0, s.deliveredBytes);
        assertEquals(0, s.deliveredPayloads);
    }

    /** byCode tracks every response; only {@code delivered=true} feeds the delivered totals. */
    @Test
    public void responses() {
        Telemetry t = new Telemetry();
        t.onResponse(200, 17, true);
        t.onResponse(200, 23, true);
        t.onResponse(400, 13, false);
        t.onTransportError(99);
        t.onTransportError(101);
        Telemetry.Snapshot s = t.snapshot(null);

        // Only delivered (200) responses count toward delivered totals.
        assertEquals(2, s.deliveredPayloads);
        assertEquals(40, s.deliveredBytes);

        // Each code has its own byCode entry; repeated calls accumulate. Transport errors are
        // bucketed under "0".
        assertCounters(2, 40, s.byCode.get("200"));
        assertCounters(1, 13, s.byCode.get("400"));
        assertCounters(2, 200, s.byCode.get("0"));

        // Codes that never saw a response don't appear.
        assertNull(s.byCode.get("404"));
    }

    @Test
    public void queueState() throws InterruptedException {
        Telemetry t = new Telemetry();
        BoundedQueue q = new BoundedQueue(100, 1, WhenFull.DROP, t);
        q.add(new byte[5]);
        q.add(new byte[7]);
        Telemetry.Snapshot s = t.snapshot(q);
        assertEquals(2, s.queuePayloads);
        assertEquals(12, s.queueBytes);
        assertEquals(100, s.queueMaxBytes);
        assertTrue(s.oldestEnqueuedAgeNanos >= 0);
    }

    /** Each snapshot captures and clears the counters so subsequent snapshots are deltas. */
    @Test
    public void deltaSemantics() {
        Telemetry t = new Telemetry();
        t.onEnqueue(10);
        t.onResponse(200, 5, true);
        t.onResponse(503, 7, false);
        t.onDrop(1, 10);
        t.onDrop(1, 25);
        Telemetry.Snapshot s1 = t.snapshot(null);
        assertEquals(1, s1.enqueuedPayloads);
        assertEquals(10, s1.enqueuedBytes);
        assertEquals(1, s1.deliveredPayloads);
        assertEquals(5, s1.deliveredBytes);
        assertEquals(2, s1.droppedPayloads);
        assertEquals(35, s1.droppedBytes);
        assertCounters(1, 5, s1.byCode.get("200"));
        assertCounters(1, 7, s1.byCode.get("503"));

        // No activity since s1 — every counter resets to zero and byCode is empty.
        Telemetry.Snapshot s2 = t.snapshot(null);
        assertEquals(0, s2.enqueuedPayloads);
        assertEquals(0, s2.enqueuedBytes);
        assertEquals(0, s2.deliveredPayloads);
        assertEquals(0, s2.deliveredBytes);
        assertEquals(0, s2.droppedPayloads);
        assertEquals(0, s2.droppedBytes);
        assertTrue(s2.byCode.isEmpty());

        // Only the new activity shows up in s3.
        t.onResponse(200, 11, true);
        Telemetry.Snapshot s3 = t.snapshot(null);
        assertEquals(1, s3.deliveredPayloads);
        assertEquals(11, s3.deliveredBytes);
        assertCounters(1, 11, s3.byCode.get("200"));
        assertNull(s3.byCode.get("503"));
    }

    /** intervalStartMillis is the wall-clock time of the previous snapshot (or construction). */
    @Test
    public void intervalStart() {
        MockClock clock = new MockClock(Instant.parse("2026-01-01T00:00:00Z"));
        Telemetry t = new Telemetry(clock, System::nanoTime);
        long constructionMillis = clock.millis();

        clock.advanceMillis(10);
        Telemetry.Snapshot s1 = t.snapshot(null);
        // First snapshot's interval starts at construction time.
        assertEquals(constructionMillis, s1.intervalStartMillis);

        clock.advanceMillis(5);
        Telemetry.Snapshot s2 = t.snapshot(null);
        // Second snapshot's interval starts at the moment of the first snapshot.
        assertEquals(constructionMillis + 10, s2.intervalStartMillis);
    }

    /**
     * lastSuccessAgeNanos = (snapshot time) − (most recent delivery); grows until next delivery.
     */
    @Test
    public void lastSuccessAge() {
        MockNanos nanos = new MockNanos(0);
        Telemetry t = new Telemetry(Clock.systemUTC(), nanos);

        // First success at t=0.
        t.onResponse(200, 1, true);

        // Window 1: age = time since the success.
        nanos.advanceMillis(7);
        assertEquals(7_000_000L, t.snapshot(null).lastSuccessAgeNanos);

        // Window 2 with no new delivery: age keeps growing.
        nanos.advanceMillis(3);
        assertEquals(10_000_000L, t.snapshot(null).lastSuccessAgeNanos);

        // A new delivery resets the reference point; age is 0 if snapshotted at the same nano.
        t.onResponse(200, 1, true);
        assertEquals(0L, t.snapshot(null).lastSuccessAgeNanos);
    }
}
