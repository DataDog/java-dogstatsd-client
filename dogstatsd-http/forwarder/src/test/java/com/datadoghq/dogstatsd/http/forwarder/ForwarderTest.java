/* Unless explicitly stated otherwise all files in this repository are
 * licensed under the Apache 2.0 License.
 *
 * This product includes software developed at Datadog
 *  (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.
 */

package com.datadoghq.dogstatsd.http.forwarder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.datadoghq.dogstatsd.http.ForwarderContext;
import java.net.URI;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

public class ForwarderTest {
    private static final URI URL = URI.create("http://localhost:0/");
    private static final Map<String, String> emptyMap = new HashMap();

    private static ForwarderContext.Builder contextBuilder() {
        return ForwarderContext.builder().environment(emptyMap).baseUri("http://localhost:8125");
    }

    private static Forwarder.Builder builder() {
        return Forwarder.builder().context(contextBuilder().build());
    }

    private static Forwarder newForwarder(long maxBytes, WhenFull whenFull) {
        return builder().maxRequestsBytes(maxBytes).maxTries(1).whenFull(whenFull).build();
    }

    @Test
    public void builderRejectsInvalidValues() {
        Forwarder.Builder b = Forwarder.builder();
        assertThrows(IllegalArgumentException.class, () -> b.maxRequestsBytes(0));
        assertThrows(IllegalArgumentException.class, () -> b.maxTries(0));
        assertThrows(NullPointerException.class, () -> b.whenFull(null));
        assertThrows(NullPointerException.class, () -> b.connectTimeout(null));
        assertThrows(IllegalArgumentException.class, () -> b.connectTimeout(Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> b.requestTimeout(Duration.ZERO));
    }

    /** A null request timeout is legal and means requests have no timeout at all. */
    @Test
    public void nullRequestTimeoutIsAllowed() {
        Forwarder f = builder().requestTimeout(null).build();
        assertNull(f.requestTimeout);
    }

    @Test
    public void contextSuppliesOriginDetectionHeaders() {
        Forwarder f =
                Forwarder.builder()
                        .context(
                                contextBuilder().localData("ci-abc").externalData("en-xyz").build())
                        .build();
        assertEquals("ci-abc", f.localData);
        assertEquals("en-xyz", f.externalData);
    }

    /** Values that can't be sent as a header value are rejected where they're supplied. */
    @Test
    public void contextRejectsUnsendableHeaderValue() {
        ForwarderContext ctx = contextBuilder().localData("bad\nvalue").build();
        Forwarder.Builder b = Forwarder.builder();
        assertThrows(IllegalArgumentException.class, () -> b.context(ctx));
    }

    @Test
    public void sendCountsEnqueue() throws InterruptedException {
        Forwarder f = newForwarder(100, WhenFull.DROP);
        f.send(URL, new byte[7]);
        f.send(URL, new byte[3]);
        Telemetry.Snapshot s = f.snapshot();
        assertEquals(2, s.enqueuedPayloads);
        assertEquals(10, s.enqueuedBytes);
    }

    @Test
    public void oversizedSendDoesNotCount() {
        Forwarder f = newForwarder(10, WhenFull.DROP);
        assertThrows(IllegalArgumentException.class, () -> f.send(URL, new byte[11]));
        Telemetry.Snapshot s = f.snapshot();
        assertEquals(0, s.enqueuedPayloads);
        assertEquals(0, s.enqueuedBytes);
    }

    /** A 400 response means the payload won't be retried, so it counts as a drop. */
    @Test
    public void handle400() throws Exception {
        Forwarder f = newForwarder(100, WhenFull.DROP);
        f.send(URL, new byte[7]);
        Map.Entry<BoundedQueue.Key, Payload> item = f.queue.next();
        f.handleResponse(400, item);
        Telemetry.Snapshot s = f.snapshot();
        assertEquals(1, s.enqueuedPayloads);
        assertEquals(7, s.enqueuedBytes);
        assertEquals(1, s.droppedPayloads);
        assertEquals(7, s.droppedBytes);
        assertEquals(0, s.deliveredPayloads);
    }

    @Test
    public void handle200() throws Exception {
        Forwarder f = newForwarder(100, WhenFull.DROP);
        f.send(URL, new byte[7]);
        Map.Entry<BoundedQueue.Key, Payload> item = f.queue.next();
        f.handleResponse(200, item);
        Telemetry.Snapshot s = f.snapshot();
        assertEquals(1, s.deliveredPayloads);
        assertEquals(7, s.deliveredBytes);
        assertEquals(0, s.droppedPayloads);
        assertEquals(0, s.queuePayloads);
        Telemetry.Snapshot.CodeCounters cc = s.byCode.get("200");
        assertNotNull(cc);
        assertEquals(1, cc.payloads);
        assertEquals(7, cc.bytes);
    }

    /** Transport errors put the item back on the queue (within {@code maxTries}) for a retry. */
    @Test
    public void handleError() throws Exception {
        Forwarder f = newForwarder(100, WhenFull.DROP);
        f.send(URL, new byte[7]);
        Map.Entry<BoundedQueue.Key, Payload> item = f.queue.next();
        f.handleTransportError(item);
        Telemetry.Snapshot s = f.snapshot();
        assertEquals(0, s.deliveredPayloads);
        assertEquals(0, s.droppedPayloads);
        assertEquals(1, s.queuePayloads);
        assertEquals(7, s.queueBytes);
        Telemetry.Snapshot.CodeCounters cc = s.byCode.get("0");
        assertNotNull(cc);
        assertEquals(1, cc.payloads);
        assertEquals(7, cc.bytes);
    }

    /** Unrecognized response codes are recorded and the item is requeued for retry. */
    @Test
    public void handle500() throws Exception {
        Forwarder f = newForwarder(100, WhenFull.DROP);
        f.send(URL, new byte[7]);
        Map.Entry<BoundedQueue.Key, Payload> item = f.queue.next();
        f.handleResponse(500, item);
        Telemetry.Snapshot s = f.snapshot();
        assertEquals(0, s.deliveredPayloads);
        assertEquals(0, s.droppedPayloads);
        assertEquals(1, s.queuePayloads);
        assertEquals(7, s.queueBytes);
        Telemetry.Snapshot.CodeCounters cc = s.byCode.get("500");
        assertNotNull(cc);
        assertEquals(1, cc.payloads);
        assertEquals(7, cc.bytes);
    }

    /** With an empty queue, close() unblocks the loop's next() and drains cleanly. */
    @Test(timeout = 5000)
    public void closeDrainsEmptyQueueReturnsTrue() throws InterruptedException {
        Forwarder f = newForwarder(100, WhenFull.DROP);
        f.start();
        assertTrue(f.close(Duration.ofSeconds(5)));
        assertFalse(f.isAlive());
    }

    /** Payloads queued before close() are all delivered before the thread exits. */
    @Test(timeout = 5000)
    public void closeDrainsPendingItemsReturnsTrue() throws InterruptedException {
        AtomicInteger processed = new AtomicInteger();
        Forwarder f =
                new Forwarder(Forwarder.builder()) {
                    @Override
                    void runOnce(Map.Entry<BoundedQueue.Key, Payload> item) {
                        processed.incrementAndGet();
                    }
                };
        f.send(URL, new byte[3]);
        f.send(URL, new byte[3]);
        f.send(URL, new byte[3]);
        f.start();
        assertTrue(f.close(Duration.ofSeconds(5)));
        assertFalse(f.isAlive());
        assertEquals(3, processed.get());
    }

    /** After close(), send() propagates the closed-queue IllegalStateException. */
    @Test
    public void sendAfterCloseThrows() throws InterruptedException {
        Forwarder f = newForwarder(100, WhenFull.DROP);
        assertTrue(f.close(Duration.ofSeconds(1)));
        assertThrows(IllegalStateException.class, () -> f.send(URL, new byte[3]));
    }

    /** If the backlog can't drain in time, close() interrupts the thread and returns false. */
    @Test(timeout = 5000)
    public void closeTimesOutReturnsFalse() throws InterruptedException {
        CountDownLatch entered = new CountDownLatch(1);
        Forwarder f =
                new Forwarder(Forwarder.builder()) {
                    @Override
                    void runOnce(Map.Entry<BoundedQueue.Key, Payload> item)
                            throws InterruptedException {
                        try {
                            entered.countDown();
                            Thread.sleep(Long.MAX_VALUE);
                        } catch (InterruptedException ex) {
                            queue.requeue(item);
                            throw ex;
                        }
                    }
                };
        f.send(URL, new byte[3]);
        f.start();
        entered.await();
        assertFalse(f.close(Duration.ofMillis(200)));
        assertFalse(f.isAlive());
    }
}
