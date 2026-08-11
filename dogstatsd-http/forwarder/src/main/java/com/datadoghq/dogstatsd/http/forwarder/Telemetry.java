/* Unless explicitly stated otherwise all files in this repository are
 * licensed under the Apache 2.0 License.
 *
 * This product includes software developed at Datadog
 *  (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.
 */

package com.datadoghq.dogstatsd.http.forwarder;

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.function.LongSupplier;

/** Thread-safe counter store for {@link Forwarder} telemetry. */
public class Telemetry {

    /** HTTP status code used to record transport-level (no-response) errors. */
    public static final int TRANSPORT_ERROR_CODE = 0;

    /** Queue state at snapshot time, plus counters for the interval leading up to it. */
    public static final class Snapshot {
        /**
         * Wall-clock time (Unix epoch milliseconds) at the start of the interval covered by this
         * snapshot — i.e., the moment of the previous snapshot, or telemetry construction if this
         * is the first snapshot.
         */
        public long intervalStartMillis;

        /** Number of payloads added to the queue in this interval. */
        public long enqueuedPayloads;

        /** Number of payloads successfully delivered in this interval. */
        public long deliveredPayloads;

        /** Total size in bytes of payloads added to the queue in this interval. */
        public long enqueuedBytes;

        /** Total size in bytes of payloads successfully delivered in this interval. */
        public long deliveredBytes;

        /** Number of payloads currently in the queue. */
        public long queuePayloads;

        /** Total size in bytes of payloads currently in the queue. */
        public long queueBytes;

        /** Maximum number of bytes the queue is allowed to store. */
        public long queueMaxBytes;

        /** Number of payloads dropped in this interval. */
        public long droppedPayloads;

        /** Total size in bytes of payloads dropped in this interval. */
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

        /** Destination for the metrics of an encoded snapshot. */
        public interface Encoder {
            /**
             * Emits a gauge.
             *
             * @param name Full metric name.
             * @param value Metric value.
             */
            void gauge(String name, double value);

            /**
             * Emits a count.
             *
             * @param name Full metric name.
             * @param value Metric value.
             * @param tags Tags to attach, or {@code null} for none.
             */
            void count(String name, double value, String[] tags);
        }

        /**
         * Encodes this snapshot into {@code enc} using the default metric name prefix.
         *
         * @param enc Encoder to emit metrics to.
         */
        public void encodeTo(Encoder enc) {
            encodeTo(DEFAULT_PREFIX, enc);
        }

        /**
         * Encodes this snapshot into {@code enc}.
         *
         * <p>Metrics carry no timestamp — the encoder decides how to timestamp them.
         *
         * @param prefix Metric name prefix.
         * @param enc Encoder to emit metrics to.
         */
        public void encodeTo(String prefix, Encoder enc) {
            enc.count(prefix + ".enqueued_payloads", (double) enqueuedPayloads, null);
            enc.count(prefix + ".enqueued_bytes", (double) enqueuedBytes, null);
            enc.count(prefix + ".delivered_payloads", (double) deliveredPayloads, null);
            enc.count(prefix + ".delivered_bytes", (double) deliveredBytes, null);
            enc.count(prefix + ".dropped_payloads", (double) droppedPayloads, null);
            enc.count(prefix + ".dropped_bytes", (double) droppedBytes, null);

            enc.gauge(prefix + ".queue_payloads", (double) queuePayloads);
            enc.gauge(prefix + ".queue_bytes", (double) queueBytes);
            enc.gauge(prefix + ".queue_max_bytes", (double) queueMaxBytes);

            enc.gauge(prefix + ".oldest_enqueued_age_seconds", oldestEnqueuedAgeNanos / 1e9);
            enc.gauge(prefix + ".last_success_age_seconds", lastSuccessAgeNanos / 1e9);

            for (Map.Entry<String, CodeCounters> e : byCode.entrySet()) {
                String[] tags = {"code:" + e.getKey()};
                CodeCounters c = e.getValue();
                enc.count(prefix + ".response_payloads", (double) c.payloads, tags);
                enc.count(prefix + ".response_bytes", (double) c.bytes, tags);
            }
        }

        /** Per-code totals within a snapshot's window. */
        public static final class CodeCounters {
            /** Number of payloads. */
            public long payloads;

            /** Total size in bytes of payloads. */
            public long bytes;
        }
    }

    private final Clock clock;
    private final LongSupplier nanos;

    private Snapshot current;

    private long lastSuccessNanos;
    private boolean everDelivered;

    Telemetry() {
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

    synchronized Snapshot snapshot(BoundedQueue q) {
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
