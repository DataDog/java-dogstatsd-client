/* Unless explicitly stated otherwise all files in this repository are
 * licensed under the Apache 2.0 License.
 *
 * This product includes software developed at Datadog
 *  (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.
 */

package com.datadoghq.dogstatsd.http.serializer;

/** Consumes payloads from the PayloadBuilder. */
public interface PayloadConsumer {
    /**
     * Called when payload builder finishes another payload.
     *
     * @param payload Completed payload.
     */
    void handle(byte[] payload);
}
