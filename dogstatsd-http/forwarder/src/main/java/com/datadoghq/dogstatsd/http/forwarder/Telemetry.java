/* Unless explicitly stated otherwise all files in this repository are
 * licensed under the Apache 2.0 License.
 *
 * This product includes software developed at Datadog
 *  (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.
 */

package com.datadoghq.dogstatsd.http.forwarder;

import com.datadoghq.dogstatsd.http.serializer.PayloadBuilder;
import java.time.Clock;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;

/** Thread-safe counter store for {@link Forwarder} telemetry. */
public class Telemetry {

    /** HTTP status code used to record transport-level (no-response) errors. */
    public static final int TRANSPORT_ERROR_CODE = 0;

    /** Point-in-time view of cumulative counters and queue state. */
    public static final class Snapshot {
        /**
         * Wall-clock time (Unix epoch milliseconds) at the start of the interval covered by this
         * snapshot — i.e., the moment of the previous snapshot, or telemetry construction if this
         * is the first snapshot.
         */
        public long intervalStartMillis;

        public long enqueuedPayloads;
        public long deliveredPayloads;
        public long enqueuedBytes;
        public long deliveredBytes;
        public long queuePayloads;
        public long queueBytes;
        public long queueMaxBytes;
        public long droppedPayloads;
        public long droppedBytes;

        /** Nanos elapsed since the oldest queued item was enqueued; {@code 0} if queue is empty. */
        public long oldestEnqueuedAgeNanos;

        /** Nanos elapsed since the last successful submission; {@code 0} if none yet. */
        public long lastSuccessAgeNanos;

        /** Totals keyed by HTTP code. */
        public Map<String, CodeCounters> byCode = new HashMap<>();

        /** Default metric name prefix used when none is supplied to {@link Snapshot#encode}. */
        static final String DEFAULT_PREFIX = "datadog.dogstatsd_http.client";

        Snapshot(long intervalStartMillis) {
            this.intervalStartMillis = intervalStartMillis;
        }

        /**
         * Encodes this snapshot into {@code pb} using the default metric.
         *
         * @param pb Builder to append metrics to.
         */
        public void encodeTo(PayloadBuilder pb) {
            encodeTo(DEFAULT_PREFIX, pb);
        }

        /**
         * Encodes this snapshot into {@code pb}.
         *
         * @param pb Builder to append metrics to.
         * @param prefix Metric name prefix.
         */
        public void encodeTo(String prefix, PayloadBuilder pb) {
            long ts = intervalStartMillis / 1000;

            pb.count(prefix + ".enqueued_payloads").addPoint(ts, enqueuedPayloads).close();
            pb.count(prefix + ".enqueued_bytes").addPoint(ts, enqueuedBytes).close();
            pb.count(prefix + ".delivered_payloads").addPoint(ts, deliveredPayloads).close();
            pb.count(prefix + ".delivered_bytes").addPoint(ts, deliveredBytes).close();
            pb.count(prefix + ".dropped_payloads").addPoint(ts, droppedPayloads).close();
            pb.count(prefix + ".dropped_bytes").addPoint(ts, droppedBytes).close();

            pb.gauge(prefix + ".queue_payloads").addPoint(ts, queuePayloads).close();
            pb.gauge(prefix + ".queue_bytes").addPoint(ts, queueBytes).close();
            pb.gauge(prefix + ".queue_max_bytes").addPoint(ts, queueMaxBytes).close();

            pb.gauge(prefix + ".oldest_enqueued_age_seconds")
                    .addPoint(ts, oldestEnqueuedAgeNanos / 1e9)
                    .close();
            pb.gauge(prefix + ".last_success_age_seconds")
                    .addPoint(ts, lastSuccessAgeNanos / 1e9)
                    .close();

            for (Map.Entry<String, CodeCounters> e : byCode.entrySet()) {
                List<String> tags = Collections.singletonList("code:" + e.getKey());
                CodeCounters c = e.getValue();
                pb.count(prefix + ".response_payloads")
                        .setTags(tags)
                        .addPoint(ts, c.payloads)
                        .close();
                pb.count(prefix + ".response_bytes").setTags(tags).addPoint(ts, c.bytes).close();
            }
        }

        /** Per-code totals within a snapshot's window. */
        public static final class CodeCounters {
            public long payloads;
            public long bytes;
        }
    }

    private final Clock clock;
    private final LongSupplier nanos;

    private Snapshot current;

    private long lastSuccessNanos;
    private boolean everDelivered;

    public Telemetry() {
        this(Clock.systemUTC(), System::nanoTime);
    }

    Telemetry(Clock clock, LongSupplier nanos) {
        this.clock = clock;
        this.nanos = nanos;
        this.current = new Snapshot(clock.millis());
    }

    synchronized void onEnqueue(int len) {
        current.enqueuedPayloads++;
        current.enqueuedBytes += len;
    }

    synchronized void onResponse(int code, int len, boolean delivered) {
        Snapshot.CodeCounters c =
                current.byCode.computeIfAbsent(
                        HttpCode.name(code), k -> new Snapshot.CodeCounters());
        c.payloads++;
        c.bytes += len;
        if (delivered) {
            current.deliveredPayloads++;
            current.deliveredBytes += len;
            lastSuccessNanos = nanos.getAsLong();
            everDelivered = true;
        }
    }

    synchronized void onTransportError(int len) {
        onResponse(TRANSPORT_ERROR_CODE, len, false);
    }

    /** Records a dropped payload. */
    synchronized void onDrop(long payloads, long bytes) {
        current.droppedPayloads += payloads;
        current.droppedBytes += bytes;
    }

    /**
     * Captures a snapshot using the supplied queue stats, then swaps in a fresh accumulator so
     * subsequent snapshots report deltas since this call.
     */
    public synchronized Snapshot snapshot(BoundedQueue q) {
        long now = nanos.getAsLong();
        Snapshot s = current;
        current = new Snapshot(clock.millis());
        s.lastSuccessAgeNanos = everDelivered ? now - lastSuccessNanos : 0L;
        if (q != null) {
            q.snapshot(now, s);
        }
        return s;
    }
}
