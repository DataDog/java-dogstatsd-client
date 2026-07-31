/* Unless explicitly stated otherwise all files in this repository are
 * licensed under the Apache 2.0 License.
 *
 * This product includes software developed at Datadog
 *  (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.
 */

package com.datadoghq.dogstatsd.http.forwarder;

import static java.net.http.HttpRequest.BodyPublishers;
import static java.net.http.HttpResponse.BodyHandlers;

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

    String localData;
    String externalData;

    final Telemetry telemetry;

    /**
     * Creates a new forwarder.
     *
     * @param maxRequestsBytes maximum total size of buffered payloads, in bytes
     * @param maxTries maximum number of delivery attempts per payload
     * @param whenFull action to take when the queue is at capacity
     * @param connectTimeout timeout for establishing the TCP connection
     * @param requestTimeout timeout from sending the request until response headers are received;
     *     {@code null} disables the request timeout
     */
    public Forwarder(
            long maxRequestsBytes,
            long maxTries,
            WhenFull whenFull,
            Duration connectTimeout,
            Duration requestTimeout) {
        this.telemetry = new Telemetry();
        this.queue = new BoundedQueue(maxRequestsBytes, maxTries, whenFull, this.telemetry);
        this.requestTimeout = requestTimeout;
        this.client =
                HttpClient.newBuilder()
                        .version(HttpClient.Version.HTTP_2)
                        .connectTimeout(connectTimeout)
                        .build();
    }

    /**
     * Captures a snapshot of the forwarder's telemetry counters and queue state, clearing delta
     * counters so subsequent snapshots report activity since this call.
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
     * <p>If the queue is full, behaviour is determined by the {@link WhenFull} policy supplied at
     * construction time.
     *
     * @param url the remote HTTP endpoint to POST the payload to
     * @param payload the raw bytes to deliver
     * @throws InterruptedException if the calling thread is interrupted while waiting for space
     *     ({@link WhenFull#BLOCK} mode only)
     * @throws IllegalStateException if the forwarder has been closed via {@link #close(Duration)}
     */
    public void send(URI url, byte[] payload) throws InterruptedException {
        Objects.requireNonNull(url, "url");
        Objects.requireNonNull(payload, "payload");
        queue.add(new Payload(url, payload));
        telemetry.onEnqueue(payload.length);
    }

    void runOnce(Map.Entry<BoundedQueue.Key, Payload> item) throws InterruptedException {
        Payload payload = item.getValue();
        logger.log(
                Level.INFO,
                "sending {0} bytes to {1}",
                new Object[] {payload.bytes.length, payload.url});

        HttpRequest.Builder builder =
                HttpRequest.newBuilder(payload.url).POST(BodyPublishers.ofByteArray(payload.bytes));
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
     * Sets the local-data value sent as the {@code x-dsd-ld} header with each request.
     *
     * <p>Local data carries the container ID or cgroup node inode used by the Datadog Agent for
     * origin detection (DogStatsD protocol v1.4).
     *
     * @param data the local-data string, or {@code null} to omit the header
     */
    public void setLocalData(String data) {
        validateHeaderValue(data);
        logger.log(Level.INFO, "using local data: {0}", data);
        localData = data;
    }

    /**
     * Sets the external-data value sent as the {@code x-dsd-ed} header with each request.
     *
     * <p>External data is supplied by the Datadog Agent Admission Controller and is used by the
     * Agent to enrich metrics with container tags when a container ID is unavailable (DogStatsD
     * protocol v1.5, Agent &ge; v7.57.0).
     *
     * @param data the external-data string, or {@code null} to omit the header
     */
    public void setExternalData(String data) {
        validateHeaderValue(data);
        logger.log(Level.INFO, "using external data: {0}", data);
        externalData = data;
    }

    private static final Pattern validHeaderValue = Pattern.compile("[\\t\\x20-\\x7E\\x80-\\xFF]*");

    private static void validateHeaderValue(String value) {
        if (value == null) {
            return;
        }
        if (!validHeaderValue.matcher(value).matches()) {
            throw new IllegalArgumentException("invalid character");
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
     *     false} if the timeout elapsed with data still queued
     * @throws InterruptedException if the calling thread is interrupted while waiting
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
}
