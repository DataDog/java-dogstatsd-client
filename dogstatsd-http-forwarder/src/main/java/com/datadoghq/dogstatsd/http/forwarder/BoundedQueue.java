/* Unless explicitly stated otherwise all files in this repository are
 * licensed under the Apache 2.0 License.
 *
 * This product includes software developed at Datadog
 *  (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.
 */

package com.datadoghq.dogstatsd.http.forwarder;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;

class BoundedQueue {
    // Key represents a tuple of integers (tries, clock). enqueuedAtNanos records when the
    // payload first entered the queue (in System.nanoTime() units) and is preserved across
    // requeues so that "age of oldest item" remains accurate after retries.
    static class Key implements Comparable<Key> {
        final long tries;
        final long clock;
        final long enqueuedAtNanos;

        Key(long clock) {
            this(0, clock, System.nanoTime());
        }

        private Key(long tries, long clock, long enqueuedAtNanos) {
            this.tries = tries;
            this.clock = clock;
            this.enqueuedAtNanos = enqueuedAtNanos;
        }

        Key next() {
            return new Key(tries + 1, clock, enqueuedAtNanos);
        }

        @Override
        public int compareTo(Key o) {
            // Keys are ordered such first we try items with fewer
            // attempts, and then with a newer (larger) clock value.
            if (tries == o.tries) {
                return Long.compare(o.clock, clock);
            }
            return Long.compare(tries, o.tries);
        }
    }

    long clock = Long.MIN_VALUE;
    long bytes;
    final long maxBytes;
    final long maxTries;
    final WhenFull whenFull;

    final TreeMap<Key, byte[]> items = new TreeMap<>();

    final Telemetry telemetry;
    final LongSupplier nanos;

    Lock lock = new ReentrantLock();
    Condition notEmpty = lock.newCondition();
    Condition notFull = lock.newCondition();

    BoundedQueue(long maxBytes, long maxTries, WhenFull whenFull) {
        this(maxBytes, maxTries, whenFull, null);
    }

    BoundedQueue(long maxBytes, long maxTries, WhenFull whenFull, Telemetry telemetry) {
        this(maxBytes, maxTries, whenFull, telemetry, System::nanoTime);
    }

    BoundedQueue(
            long maxBytes,
            long maxTries,
            WhenFull whenFull,
            Telemetry telemetry,
            LongSupplier nanos) {
        this.maxBytes = maxBytes;
        this.maxTries = maxTries;
        this.whenFull = whenFull;
        this.telemetry = telemetry;
        this.nanos = nanos;
    }

    long droppedPayloads;
    long droppedBytes;

    private void recordDrop(long bytes) {
        droppedPayloads++;
        droppedBytes += bytes;
    }

    void add(byte[] item) throws InterruptedException {
        put(null, item, whenFull);
    }

    void requeue(Map.Entry<Key, byte[]> item) throws InterruptedException {
        Key nextKey = item.getKey().next();
        if (nextKey.tries > maxTries) {
            telemetry.onDrop(1, item.getValue().length);
            return;
        }
        put(nextKey, item.getValue(), WhenFull.DROP);
    }

    // Must be called when lock is held.
    private Key newKey() {
        clock++;
        return new Key(0, clock, nanos.getAsLong());
    }

    private void put(Key key, byte[] item, WhenFull whenFull) throws InterruptedException {
        lock.lock();
        try {
            if (key == null) {
                key = newKey();
            }
            ensureSpace(item.length, whenFull);
            items.put(key, item);
            bytes += item.length;
            notEmpty.signal();
        } finally {
            long droppedPayloads = this.droppedPayloads;
            long droppedBytes = this.droppedBytes;
            this.droppedPayloads = 0;
            this.droppedBytes = 0;
            lock.unlock();
            // Avoid potential lock ordering issues.
            telemetry.onDrop(droppedPayloads, droppedBytes);
        }
    }

    private void ensureSpace(int length, WhenFull whenFull) throws InterruptedException {
        if (length > maxBytes) {
            throw new IllegalArgumentException("item length is larger than maxBytes");
        }
        while (bytes + length > maxBytes) {
            switch (whenFull) {
                case DROP:
                    Map.Entry<Key, byte[]> last = items.pollLastEntry();
                    bytes -= last.getValue().length;
                    recordDrop(last.getValue().length);
                    break;
                case BLOCK:
                    notFull.await();
                    break;
            }
        }
    }

    Map.Entry<Key, byte[]> next() throws InterruptedException {
        lock.lock();
        try {
            while (items.size() == 0) {
                notEmpty.await();
            }
            Map.Entry<Key, byte[]> item = items.pollFirstEntry();
            bytes -= item.getValue().length;
            notFull.signalAll();
            return item;
        } finally {
            lock.unlock();
        }
    }

    void snapshot(long now, Telemetry.Snapshot s) {
        lock.lock();
        try {
            long oldestAge = 0L;
            for (Key k : items.keySet()) {
                long age = now - k.enqueuedAtNanos;
                if (age > oldestAge) {
                    oldestAge = age;
                }
            }
            s.queuePayloads = items.size();
            s.queueBytes = bytes;
            s.queueMaxBytes = maxBytes;
            s.oldestEnqueuedAgeNanos = oldestAge;
        } finally {
            lock.unlock();
        }
    }
}
