/* Unless explicitly stated otherwise all files in this repository are
 * licensed under the Apache 2.0 License.
 *
 * This product includes software developed at Datadog
 *  (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.
 */

package com.datadoghq.dogstatsd.http.forwarder;

import static java.net.http.HttpRequest.BodyPublishers;
import static java.net.http.HttpResponse.BodyHandlers;

import com.datadoghq.dogstatsd.http.ForwarderContext;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

/**
 * An HTTP forwarder that delivers DogStatsD HTTP payloads to a remote endpoint.
 *
 * <p>Payloads are enqueued via {@link #send(URI, byte[])} and delivered asynchronously by a
 * background thread. Failed requests are retried with exponential back-off up to {@code maxTries}
 * attempts before being discarded.
 */
public class Forwarder extends Thread {
    static final Logger logger = Logger.getLogger(Forwarder.class.getName());
    final BoundedQueue queue;
    final HttpClient client;
    final Duration requestTimeout;
    final Random rng = new Random();

    final URI baseUri;
    final String localData;
    final String externalData;

    final Telemetry telemetry;

    /**
     * Creates a builder for a forwarder.
     *
     * @return a new builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    Forwarder(final Builder builder) {
        this.telemetry = new Telemetry();
        this.queue =
                new BoundedQueue(
                        builder.maxRequestsBytes,
                        builder.maxTries,
                        builder.whenFull,
                        this.telemetry);
        this.requestTimeout = builder.requestTimeout;
        this.baseUri = builder.baseUri;
        this.localData = builder.localData;
        this.externalData = builder.externalData;

        this.client =
                HttpClient.newBuilder()
                        .version(HttpClient.Version.HTTP_2)
                        .connectTimeout(builder.connectTimeout)
                        .build();
    }

    /**
     * Captures a snapshot of the forwarder's telemetry counters and queue state, clearing delta
     * counters so subsequent snapshots report activity since this call.
     *
     * @return a telemetry snapshot.
     */
    public Telemetry.Snapshot snapshot() {
        return telemetry.snapshot(queue);
    }

    /** Runs the forwarding loop, delivering queued payloads until the thread is interrupted. */
    @Override
    public void run() {
        while (!interrupted()) {
            try {
                Map.Entry<BoundedQueue.Key, Payload> item = queue.next();
                if (item == null) {
                    return;
                }
                runOnce(item);
            } catch (InterruptedException e) {
                return;
            } catch (Exception t) {
                logger.log(Level.SEVERE, "unexpected error in forwarder loop", t);
            }
        }
    }

    /**
     * Enqueues a payload for delivery to the given endpoint.
     *
     * <p>If the queue is full, behavior is determined by the {@link WhenFull} policy set with
     * {@link Builder#whenFull}.
     *
     * @param url the remote HTTP endpoint to POST the payload to.
     * @param payload the raw bytes to deliver.
     * @throws InterruptedException if the calling thread is interrupted while waiting for space
     *     ({@link WhenFull#BLOCK} mode only).
     * @throws IllegalStateException if the forwarder has been closed via {@link #close(Duration)}.
     */
    public void send(URI url, byte[] payload) throws InterruptedException {
        Objects.requireNonNull(url, "url");
        Objects.requireNonNull(payload, "payload");
        queue.add(new Payload(url, payload));
        telemetry.onEnqueue(payload.length);
    }

    void runOnce(Map.Entry<BoundedQueue.Key, Payload> item) throws InterruptedException {
        Payload payload = item.getValue();
        final URI url = baseUri.resolve(payload.url);
        logger.log(
                Level.INFO, "sending {0} bytes to {1}", new Object[] {payload.bytes.length, url});

        HttpRequest.Builder builder =
                HttpRequest.newBuilder(url).POST(BodyPublishers.ofByteArray(payload.bytes));
        if (requestTimeout != null) {
            builder.timeout(requestTimeout);
        }
        if (localData != null) {
            builder.setHeader("x-dsd-ld", localData);
        }
        if (externalData != null) {
            builder.setHeader("x-dsd-ed", externalData);
        }
        HttpRequest req = builder.build();

        try {
            HttpResponse<String> res = client.send(req, BodyHandlers.ofString());
            res.body();

            logger.log(
                    Level.INFO, "response {0}: {1}", new Object[] {res.statusCode(), res.body()});

            handleResponse(res.statusCode(), item);
        } catch (IOException ex) {
            logger.log(Level.WARNING, "error sending request: {0}", ex.toString());
            handleTransportError(item);
        } catch (InterruptedException ex) {
            // Wouldn't be retried, but will show up as a leftover in a telemetry snapshot.
            queue.requeue(item);
            throw ex;
        }

        backoff();
    }

    void handleResponse(int code, Map.Entry<BoundedQueue.Key, Payload> item)
            throws InterruptedException {
        int len = item.getValue().bytes.length;
        switch (code) {
            case 400:
                telemetry.onResponse(code, len, false);
                telemetry.onDrop(1, len);
                decreaseBackoff();
                break;
            case 200:
                telemetry.onResponse(code, len, true);
                decreaseBackoff();
                break;
            default:
                telemetry.onResponse(code, len, false);
                increaseBackoff();
                queue.requeue(item);
        }
    }

    void handleTransportError(Map.Entry<BoundedQueue.Key, Payload> item)
            throws InterruptedException {
        telemetry.onTransportError(item.getValue().bytes.length);
        increaseBackoff();
        queue.requeue(item);
    }

    int delay;

    void decreaseBackoff() {
        delay >>= 4;
    }

    void increaseBackoff() {
        if (delay < 64) delay <<= 1;
        if (delay == 0) delay = 1;
    }

    void backoff() throws InterruptedException {
        if (delay > 0) {
            int sleep = (int) (250.0 * delay * (0.5 + rng.nextDouble()));
            logger.log(Level.INFO, "backoff={0}, sleeping {1}ms", new Object[] {delay, sleep});
            Thread.sleep(sleep);
        }
    }

    /**
     * Closes the forwarder: stops accepting new payloads and drains the remaining backlog.
     *
     * <p>Already-queued payloads keep being delivered until either the queue drains or {@code
     * timeout} elapses, whichever comes first. If the timeout elapses first the forwarding thread
     * is interrupted, abandoning any unsent payloads.
     *
     * @param timeout maximum time to wait for the backlog to drain. {@code null} means wait
     *     forever.
     * @return {@code true} if the queue drained cleanly with no unsent payloads remaining; {@code
     *     false} if the timeout elapsed with data still queued.
     * @throws InterruptedException if the calling thread is interrupted while waiting.
     */
    public boolean close(Duration timeout) throws InterruptedException {
        queue.close();
        if (timeout == null) {
            join(0);
        } else {
            long timeoutMs = timeout.toMillis();
            if (timeoutMs > 0) {
                join(timeoutMs);
            }
        }
        if (isAlive()) {
            interrupt();
            join();
        }
        return queue.empty();
    }

    /** Builds a {@link Forwarder}. Obtained via {@link Forwarder#builder}. */
    public static final class Builder {
        private long maxRequestsBytes = 8L * 1024 * 1024;
        private long maxTries = 20;
        private WhenFull whenFull = WhenFull.DROP;
        private Duration connectTimeout = Duration.ofSeconds(1);
        private Duration requestTimeout = Duration.ofSeconds(1);
        private String localData;
        private String externalData;
        private boolean contextSet;
        private URI baseUri;

        private Builder() {}

        /**
         * Sets the maximum total size of buffered payloads, in bytes. Defaults to 8 MiB.
         *
         * <p>Payloads larger than this are rejected by {@link Forwarder#send}.
         *
         * @param val the maximum number of buffered bytes; must be positive.
         * @return this builder.
         */
        public Builder maxRequestsBytes(final long val) {
            if (val <= 0) {
                throw new IllegalArgumentException("maxRequestsBytes must be positive");
            }
            maxRequestsBytes = val;
            return this;
        }

        /**
         * Sets the maximum number of delivery attempts per payload. Defaults to 20.
         *
         * @param val the maximum number of attempts; must be at least 1.
         * @return this builder.
         */
        public Builder maxTries(final long val) {
            if (val < 1) {
                throw new IllegalArgumentException("maxTries must be at least 1");
            }
            maxTries = val;
            return this;
        }

        /**
         * Sets the action to take when the queue is at capacity. Defaults to {@link WhenFull#DROP}.
         *
         * @param val the action to take.
         * @return this builder.
         */
        public Builder whenFull(final WhenFull val) {
            whenFull = Objects.requireNonNull(val, "whenFull");
            return this;
        }

        /**
         * Sets the timeout for establishing the TCP connection. Defaults to one second.
         *
         * @param val the connect timeout; must be positive.
         * @return this builder.
         */
        public Builder connectTimeout(final Duration val) {
            Objects.requireNonNull(val, "connectTimeout");
            if (val.isNegative() || val.isZero()) {
                throw new IllegalArgumentException("connectTimeout must be positive");
            }
            connectTimeout = val;
            return this;
        }

        /**
         * Sets the timeout from sending the request until response headers are received. Defaults
         * to one second.
         *
         * @param val the request timeout, or {@code null} to disable it; must be positive when
         *     non-null.
         * @return this builder.
         */
        public Builder requestTimeout(final Duration val) {
            if (val != null && (val.isNegative() || val.isZero())) {
                throw new IllegalArgumentException("requestTimeout must be positive");
            }
            requestTimeout = val;
            return this;
        }

        /**
         * Sets the shared context for this forwarder.
         *
         * <p>Defaults to {@code ForwarderContext.defaults()}.
         *
         * @param context the context to take the values from.
         * @return this builder.
         */
        public Builder context(final ForwarderContext context) {
            contextSet = true;
            Objects.requireNonNull(context);
            baseUri = context.baseUri();
            localData = validateHeaderValue(context.localData());
            externalData = validateHeaderValue(context.externalData());
            return this;
        }

        /**
         * Builds the forwarder. The returned forwarder is a {@link Thread} that has not been
         * started yet.
         *
         * @return a new forwarder.
         */
        public Forwarder build() {
            if (!contextSet) {
                context(ForwarderContext.defaults());
            }
            return new Forwarder(this);
        }

        private static final Pattern validHeaderValue =
                Pattern.compile("[\\t\\x20-\\x7E\\x80-\\xFF]*");

        private static String validateHeaderValue(final String value) {
            if (value != null && !validHeaderValue.matcher(value).matches()) {
                throw new IllegalArgumentException("invalid character");
            }
            return value;
        }
    }
}
