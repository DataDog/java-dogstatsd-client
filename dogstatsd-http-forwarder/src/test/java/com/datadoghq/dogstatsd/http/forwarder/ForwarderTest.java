/* Unless explicitly stated otherwise all files in this repository are
 * licensed under the Apache 2.0 License.
 *
 * This product includes software developed at Datadog
 *  (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.
 */

package com.datadoghq.dogstatsd.http.forwarder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

import java.net.URI;
import java.time.Duration;
import java.util.Map;
import org.junit.Test;

public class ForwarderTest {

    private static Forwarder newForwarder(long maxBytes, WhenFull whenFull) {
        return new Forwarder(
                URI.create("http://localhost:0/"),
                maxBytes,
                1,
                whenFull,
                Duration.ofSeconds(1),
                Duration.ofSeconds(1));
    }

    @Test
    public void sendCountsEnqueue() throws InterruptedException {
        Forwarder f = newForwarder(100, WhenFull.DROP);
        f.send(new byte[7]);
        f.send(new byte[3]);
        Telemetry.Snapshot s = f.snapshot();
        assertEquals(2, s.enqueuedPayloads);
        assertEquals(10, s.enqueuedBytes);
    }

    @Test
    public void oversizedSendDoesNotCount() {
        Forwarder f = newForwarder(10, WhenFull.DROP);
        assertThrows(IllegalArgumentException.class, () -> f.send(new byte[11]));
        Telemetry.Snapshot s = f.snapshot();
        assertEquals(0, s.enqueuedPayloads);
        assertEquals(0, s.enqueuedBytes);
    }

    /** A 400 response means the payload won't be retried, so it counts as a drop. */
    @Test
    public void handle400() throws Exception {
        Forwarder f = newForwarder(100, WhenFull.DROP);
        f.send(new byte[7]);
        Map.Entry<BoundedQueue.Key, byte[]> item = f.queue.next();
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
        f.send(new byte[7]);
        Map.Entry<BoundedQueue.Key, byte[]> item = f.queue.next();
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
        f.send(new byte[7]);
        Map.Entry<BoundedQueue.Key, byte[]> item = f.queue.next();
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
        f.send(new byte[7]);
        Map.Entry<BoundedQueue.Key, byte[]> item = f.queue.next();
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
}
