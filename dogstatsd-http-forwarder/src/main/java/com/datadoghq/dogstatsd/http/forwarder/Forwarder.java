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
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An HTTP forwarder that delivers DogStatsD HTTP payloads to a remote endpoint.
 *
 * <p>Payloads are enqueued via {@link #send(byte[])} and delivered asynchronously by a background
 * thread. Failed requests are retried with exponential back-off up to {@code maxTries} attempts
 * before being discarded.
 */
public class Forwarder extends Thread {
    static final Logger logger = Logger.getLogger(Forwarder.class.getName());
    final BoundedQueue queue;
    final URI url;
    final HttpClient client;
    final Duration requestTimeout;
    final Random rng = new Random();

    String localData;
    String externalData;

    int responseOk, responseBadRequest, responseOther;

    /**
     * Creates a new forwarder targeting the given URL.
     *
     * @param url the remote HTTP endpoint to POST payloads to
     * @param maxRequestsBytes maximum total size of buffered payloads, in bytes
     * @param maxTries maximum number of delivery attempts per payload
     * @param whenFull action to take when the queue is at capacity
     * @param connectTimeout timeout for establishing the TCP connection
     * @param requestTimeout timeout from sending the request until response headers are received;
     *     {@code null} disables the request timeout
     */
    public Forwarder(
            URI url,
            long maxRequestsBytes,
            long maxTries,
            WhenFull whenFull,
            Duration connectTimeout,
            Duration requestTimeout) {
        this.url = url;
        this.queue = new BoundedQueue(maxRequestsBytes, maxTries, whenFull);
        this.requestTimeout = requestTimeout;
        this.client =
                HttpClient.newBuilder()
                        .version(HttpClient.Version.HTTP_2)
                        .connectTimeout(connectTimeout)
                        .build();
    }

    /** Runs the forwarding loop, delivering queued payloads until the thread is interrupted. */
    @Override
    public void run() {
        try {
            while (true) {
                runOnce(queue.next());
            }
        } catch (InterruptedException e) {
            return;
        }
    }

    /**
     * Enqueues a payload for delivery to the remote endpoint.
     *
     * <p>If the queue is full, behaviour is determined by the {@link WhenFull} policy supplied at
     * construction time.
     *
     * @param payload the raw bytes to deliver
     * @throws InterruptedException if the calling thread is interrupted while waiting for space
     *     ({@link WhenFull#BLOCK} mode only)
     */
    public void send(byte[] payload) throws InterruptedException {
        queue.add(payload);
    }

    void runOnce(Map.Entry<BoundedQueue.Key, byte[]> item) throws InterruptedException {
        byte[] payload = item.getValue();
        logger.log(Level.INFO, "sending {0} bytes", payload.length);

        HttpRequest.Builder builder =
                HttpRequest.newBuilder(url).POST(BodyPublishers.ofByteArray(payload));
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

            switch (res.statusCode()) {
                case 400:
                    responseBadRequest++;
                    onSuccess();
                    break;
                case 200:
                    responseOk++;
                    onSuccess();
                    break;
                default:
                    responseOther++;
                    onError();
                    queue.requeue(item);
            }
        } catch (IOException ex) {
            logger.log(Level.WARNING, "error sending request: {0}", ex.toString());
            responseOther++;
            onError();
            queue.requeue(item);
        }

        backoff();
    }

    int delay;

    void onSuccess() {
        delay >>= 4;
    }

    void onError() {
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
        logger.log(Level.INFO, "using external data: {0}", data);
        externalData = data;
    }
}
