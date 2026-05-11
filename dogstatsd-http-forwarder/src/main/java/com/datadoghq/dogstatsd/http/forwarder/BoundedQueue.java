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

class BoundedQueue {
    // Key represents a tuple of integers (tries, clock).
    static class Key implements Comparable<Key> {
        final long tries;
        final long clock;

        Key(long clock) {
            this.tries = 0;
            this.clock = clock;
        }

        private Key(long tries, long clock) {
            this.tries = tries;
            this.clock = clock;
        }

        Key next() {
            return new Key(tries + 1, clock);
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

    long droppedItems;
    long droppedBytes;

    Lock lock = new ReentrantLock();
    Condition notEmpty = lock.newCondition();
    Condition notFull = lock.newCondition();

    BoundedQueue(long maxBytes, long maxTries, WhenFull whenFull) {
        this.maxBytes = maxBytes;
        this.maxTries = maxTries;
        this.whenFull = whenFull;
    }

    void add(byte[] item) throws InterruptedException {
        put(null, item, whenFull);
    }

    void requeue(Map.Entry<Key, byte[]> item) throws InterruptedException {
        Key nextKey = item.getKey().next();
        if (nextKey.tries > maxTries) {
            droppedItems++;
            droppedBytes += item.getValue().length;
            return;
        }
        put(nextKey, item.getValue(), WhenFull.DROP);
    }

    // Must be called when lock is held.
    private Key newKey() {
        clock++;
        return new Key(clock);
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
            lock.unlock();
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
                    droppedItems++;
                    droppedBytes += last.getValue().length;
                    bytes -= last.getValue().length;
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
}
