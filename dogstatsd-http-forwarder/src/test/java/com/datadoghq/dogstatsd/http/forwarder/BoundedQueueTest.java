/* Unless explicitly stated otherwise all files in this repository are
 * licensed under the Apache 2.0 License.
 *
 * This product includes software developed at Datadog
 *  (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.
 */

package com.datadoghq.dogstatsd.http.forwarder;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Map;
import org.junit.Test;

public class BoundedQueueTest {
    private static byte[] bytes(int n) {
        return bytes(n, (byte) 0);
    }

    private static byte[] bytes(int n, byte v) {
        byte[] b = new byte[n];
        Arrays.fill(b, v);
        return b;
    }

    // --- Round-trip / bytes tracking ---

    @Test
    public void addThenNextReturnsItem() throws InterruptedException {
        BoundedQueue q = new BoundedQueue(10, 1, WhenFull.DROP, new Telemetry());
        byte[] item = bytes(4);
        q.add(item);
        Map.Entry<BoundedQueue.Key, byte[]> entry = q.next();
        assertSame(item, entry.getValue());
        assertEquals(0, q.bytes);
    }

    @Test
    public void bytesDecrementedOnNext() throws InterruptedException {
        BoundedQueue q = new BoundedQueue(30, 1, WhenFull.DROP, new Telemetry());
        q.add(bytes(3));
        q.add(bytes(3));
        q.add(bytes(3));
        assertEquals(9, q.bytes);
        q.next();
        assertEquals(6, q.bytes);
        q.next();
        assertEquals(3, q.bytes);
        q.next();
        assertEquals(0, q.bytes);
    }

    @Test
    public void newestItemDequeuesFirstWithinSameTries() throws InterruptedException {
        BoundedQueue q = new BoundedQueue(30, 1, WhenFull.DROP, new Telemetry());
        byte[] a = {1};
        byte[] b = {2};
        byte[] c = {3};
        q.add(a);
        q.add(b);
        q.add(c);
        // Within tries=0, larger clock (added later) sorts first → LIFO
        assertSame(c, q.next().getValue());
        assertSame(b, q.next().getValue());
        assertSame(a, q.next().getValue());
    }

    // --- WhenFull.DROP ---

    @Test
    public void dropWhenFullDropsOldestItem() throws InterruptedException {
        Telemetry t = new Telemetry();
        BoundedQueue q = new BoundedQueue(10, 1, WhenFull.DROP, t);
        byte[] a = bytes(5); // added first → smallest clock → last in TreeMap
        byte[] b = bytes(4);
        q.add(a); // queue: a(clock=MIN+1)
        q.add(b); // queue full: a, b
        byte[] c = bytes(3);
        q.add(c); // a (oldest, last entry) evicted
        Telemetry.Snapshot s = t.snapshot(q);
        assertEquals(1, s.droppedPayloads);
        assertEquals(5, s.droppedBytes);
        // Remaining: c (newest, clock=MIN+3) then b (clock=MIN+2)
        assertSame(c, q.next().getValue());
        assertSame(b, q.next().getValue());
    }

    @Test
    public void dropCountersAccumulate() throws InterruptedException {
        Telemetry t = new Telemetry();
        BoundedQueue q = new BoundedQueue(5, 1, WhenFull.DROP, t);
        q.add(bytes(5)); // fills queue (X)
        q.add(bytes(5)); // X dropped (Y in)
        q.add(bytes(5)); // Y dropped (Z in)
        Telemetry.Snapshot s = t.snapshot(q);
        assertEquals(2, s.droppedPayloads);
        assertEquals(10, s.droppedBytes);
    }

    @Test(timeout = 3000)
    public void dropDoesNotBlock() throws InterruptedException {
        BoundedQueue q = new BoundedQueue(5, 1, WhenFull.DROP, new Telemetry());
        q.add(bytes(5)); // fill
        q.add(bytes(5)); // should return immediately via DROP
    }

    // --- WhenFull.BLOCK ---

    @Test(timeout = 5000)
    public void blockUnblocksWhenSpaceFreed() throws InterruptedException {
        Telemetry t = new Telemetry();
        BoundedQueue q = new BoundedQueue(5, 1, WhenFull.BLOCK, t);
        q.add(bytes(5)); // queue full

        Thread producer =
                new Thread(
                        () -> {
                            try {
                                q.add(bytes(5));
                            } catch (InterruptedException e) {
                                return;
                            }
                        });
        producer.start();

        // Give producer time to reach await()
        while (!(producer.getState() == Thread.State.WAITING
                || producer.getState() == Thread.State.TIMED_WAITING)) {
            Thread.sleep(50);
        }

        q.next(); // frees space, signals notFull
        producer.join(2000);
        assertFalse(producer.isAlive());
        assertEquals(5, q.bytes);
        assertEquals(0, t.snapshot(q).droppedPayloads);
    }

    @Test(expected = IllegalArgumentException.class)
    public void addThrowsForOversizedItem() throws InterruptedException {
        BoundedQueue q = new BoundedQueue(4, 1, WhenFull.DROP, new Telemetry());
        q.add(bytes(5));
    }

    @Test
    public void requeueIncrementsTriesPreservesClock() throws InterruptedException {
        BoundedQueue q = new BoundedQueue(20, 3, WhenFull.DROP, new Telemetry());
        q.add(bytes(4));
        Map.Entry<BoundedQueue.Key, byte[]> entry = q.next();
        assertEquals(0, entry.getKey().tries);
        long originalClock = entry.getKey().clock;
        long originalEnqueued = entry.getKey().enqueuedAtNanos;

        q.requeue(entry);
        Map.Entry<BoundedQueue.Key, byte[]> requeued = q.next();
        assertEquals(1, requeued.getKey().tries);
        assertEquals(originalClock, requeued.getKey().clock);
        assertEquals(originalEnqueued, requeued.getKey().enqueuedAtNanos);
    }

    @Test
    public void requeuedItemDequeuesAfterFreshItems() throws InterruptedException {
        MockNanos nanos = new MockNanos(0);
        BoundedQueue q = new BoundedQueue(20, 3, WhenFull.DROP, new Telemetry(), nanos);
        byte[] a = {10};
        byte[] b = {20};
        q.add(a); // A.enqueuedAtNanos = 0
        Map.Entry<BoundedQueue.Key, byte[]> entryA = q.next();
        q.requeue(entryA); // A now has tries=1

        nanos.advanceMillis(5);
        q.add(b); // B has tries=0 → higher priority

        // Snapshot sees A as the oldest even though B was added later.
        Telemetry.Snapshot s = new Telemetry.Snapshot(0L);
        q.snapshot(nanos.getAsLong(), s);
        assertEquals(2, s.queuePayloads);
        assertEquals(5_000_000L, s.oldestEnqueuedAgeNanos);

        assertSame(b, q.next().getValue()); // B first (fewer tries)
        assertSame(a, q.next().getValue()); // A second
    }

    @Test
    public void requeueAtMaxTriesIsAccepted() throws InterruptedException {
        Telemetry t = new Telemetry();
        BoundedQueue q = new BoundedQueue(20, 2, WhenFull.DROP, t);
        q.add(bytes(3));
        Map.Entry<BoundedQueue.Key, byte[]> e = q.next();
        q.requeue(e); // tries → 1
        e = q.next();
        q.requeue(e); // tries → 2 == maxTries, should be accepted
        assertEquals(0, t.snapshot(q).droppedPayloads);
        assertFalse(q.items.isEmpty());
    }

    @Test
    public void requeuePastMaxTriesDropsItem() throws InterruptedException {
        Telemetry t = new Telemetry();
        BoundedQueue q = new BoundedQueue(20, 2, WhenFull.DROP, t);
        byte[] item = bytes(7);
        q.add(item);
        Map.Entry<BoundedQueue.Key, byte[]> e = q.next();
        q.requeue(e); // tries → 1
        e = q.next();
        q.requeue(e); // tries → 2 == maxTries, accepted
        e = q.next();
        q.requeue(e); // tries → 3 > maxTries, dropped
        Telemetry.Snapshot s = t.snapshot(q);
        assertEquals(1, s.droppedPayloads);
        assertEquals(7, s.droppedBytes);
        assertEquals(0, q.bytes);
        assertTrue(q.items.isEmpty());
    }

    // --- snapshot ---

    @Test
    public void snapshotEmptyQueue() {
        BoundedQueue q = new BoundedQueue(20, 1, WhenFull.DROP, new Telemetry());
        Telemetry.Snapshot s = new Telemetry.Snapshot(0L);
        q.snapshot(System.nanoTime(), s);
        assertEquals(0, s.queuePayloads);
        assertEquals(0, s.queueBytes);
        assertEquals(20, s.queueMaxBytes);
        assertEquals(0L, s.oldestEnqueuedAgeNanos);
    }

    @Test
    public void snapshotReports() throws InterruptedException {
        BoundedQueue q = new BoundedQueue(30, 1, WhenFull.DROP, new Telemetry());
        q.add(bytes(3));
        q.add(bytes(5));

        Telemetry.Snapshot s = new Telemetry.Snapshot(0L);
        q.snapshot(System.nanoTime(), s);
        assertEquals(2, s.queuePayloads);
        assertEquals(8, s.queueBytes);
        assertEquals(30, s.queueMaxBytes);
        // The oldest item was enqueued before the snapshot, so age must be positive.
        assertTrue(s.oldestEnqueuedAgeNanos > 0);
    }

    @Test(timeout = 5000)
    public void nextBlocksUntilItemAdded() throws InterruptedException {
        BoundedQueue q = new BoundedQueue(100, 1, WhenFull.DROP, new Telemetry());
        byte[] item = bytes(3);
        Map.Entry<BoundedQueue.Key, byte[]>[] result = new Map.Entry[1];

        Thread consumer =
                new Thread(
                        () -> {
                            try {
                                result[0] = q.next();
                            } catch (InterruptedException e) {
                                return;
                            }
                        });
        consumer.start();

        while (!(consumer.getState() == Thread.State.WAITING
                || consumer.getState() == Thread.State.TIMED_WAITING)) {
            Thread.sleep(50);
        }

        q.add(item);
        consumer.join(2000);
        assertFalse(consumer.isAlive());
        assertSame(item, result[0].getValue());
    }
}
