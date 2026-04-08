/* Unless explicitly stated otherwise all files in this repository are
 * licensed under the Apache 2.0 License.
 *
 * This product includes software developed at Datadog
 *  (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.
 */

package com.datadoghq.dogstatsd.http.forwarder;

/**
 * Controls the behavior of {@link Forwarder} when its internal queue is full.
 */
public enum WhenFull {
    /** Block the caller until space becomes available in the queue. */
    BLOCK,
    /** Drop a payload to make room, according to the queue's eviction policy. */
    DROP;
}
