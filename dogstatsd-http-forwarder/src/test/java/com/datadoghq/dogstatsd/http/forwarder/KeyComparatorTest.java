/* Unless explicitly stated otherwise all files in this repository are
 * licensed under the Apache 2.0 License.
 *
 * This product includes software developed at Datadog
 *  (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.
 */

package com.datadoghq.dogstatsd.http.forwarder;

import static org.junit.Assert.*;

import org.junit.Test;

public class KeyComparatorTest {

    private static BoundedQueue.Key key(long clock) {
        return new BoundedQueue.Key(clock);
    }

    // --- Reflexivity ---

    @Test
    public void reflexive() {
        BoundedQueue.Key k = key(5);
        assertEquals(0, k.compareTo(k));
    }

    // --- Antisymmetry ---

    @Test
    public void antisymmetric_sameTries_differentClock() {
        BoundedQueue.Key k1 = key(10);
        BoundedQueue.Key k2 = key(20);
        assertEquals(-Integer.signum(k1.compareTo(k2)), Integer.signum(k2.compareTo(k1)));
    }

    @Test
    public void antisymmetric_differentTries() {
        BoundedQueue.Key k0 = key(5);
        BoundedQueue.Key k1 = k0.next();
        assertEquals(-Integer.signum(k0.compareTo(k1)), Integer.signum(k1.compareTo(k0)));
    }

    // --- Transitivity ---

    @Test
    public void transitive_triesOrdering() {
        BoundedQueue.Key k0 = key(1);
        BoundedQueue.Key k1 = k0.next();
        BoundedQueue.Key k2 = k1.next();
        assertTrue(k0.compareTo(k1) < 0);
        assertTrue(k1.compareTo(k2) < 0);
        assertTrue(k0.compareTo(k2) < 0);
    }

    @Test
    public void transitive_clockOrdering() {
        // Within tries=0: larger clock → smaller compareTo → sorts first
        BoundedQueue.Key k30 = key(30);
        BoundedQueue.Key k20 = key(20);
        BoundedQueue.Key k10 = key(10);
        assertTrue(k30.compareTo(k20) < 0);
        assertTrue(k20.compareTo(k10) < 0);
        assertTrue(k30.compareTo(k10) < 0);
    }

    // --- Ordering rules ---

    @Test
    public void fewerTriesIsLess() {
        BoundedQueue.Key k0 = key(1);
        BoundedQueue.Key k1 = k0.next();
        BoundedQueue.Key k2 = k1.next();
        assertTrue(k0.compareTo(k1) < 0);
        assertTrue(k1.compareTo(k2) < 0);
    }

    @Test
    public void triesDominatesClockValue() {
        BoundedQueue.Key lowTriesLowClock = key(1); // tries=0, clock=1
        BoundedQueue.Key highTriesHighClock = key(9999).next(); // tries=1, clock=9999
        assertTrue(lowTriesLowClock.compareTo(highTriesHighClock) < 0);
    }

    @Test
    public void sameTries_largerClockSortsFirst() {
        BoundedQueue.Key kBig = key(100); // tries=0, clock=100 → sorts before kSmall
        BoundedQueue.Key kSmall = key(1); // tries=0, clock=1
        assertTrue(kBig.compareTo(kSmall) < 0);
    }

    @Test
    public void sameTries_equalClockReturnsZero() {
        BoundedQueue.Key k1 = key(50);
        BoundedQueue.Key k2 = key(50);
        assertEquals(0, k1.compareTo(k2));
    }

    // --- next() behaviour ---

    @Test
    public void nextIncrementsTries() {
        BoundedQueue.Key k = key(42);
        assertEquals(0, k.tries);
        assertEquals(42, k.clock);

        BoundedQueue.Key k1 = k.next();
        assertEquals(1, k1.tries);
        assertEquals(42, k1.clock);

        BoundedQueue.Key k2 = k1.next();
        assertEquals(2, k2.tries);
        assertEquals(42, k2.clock);
    }

    @Test
    public void nextDoesNotMutateOriginal() {
        BoundedQueue.Key k = key(7);
        k.next();
        assertEquals(0, k.tries);
        assertEquals(7, k.clock);
    }
}
