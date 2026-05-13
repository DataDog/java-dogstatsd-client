/* Unless explicitly stated otherwise all files in this repository are
 * licensed under the Apache 2.0 License.
 *
 * This product includes software developed at Datadog
 *  (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.
 */

package com.datadoghq.dogstatsd.http.forwarder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.net.URI;
import java.time.Duration;
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
}
