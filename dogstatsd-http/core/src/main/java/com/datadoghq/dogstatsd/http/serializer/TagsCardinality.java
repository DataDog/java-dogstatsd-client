/* Unless explicitly stated otherwise all files in this repository are
 * licensed under the Apache 2.0 License.
 *
 * This product includes software developed at Datadog
 *  (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.
 */

package com.datadoghq.dogstatsd.http.serializer;

/**
 * Cardinality of the origin tags the agent attaches to a metric.
 *
 * <p>See the <a
 * href="https://docs.datadoghq.com/getting_started/tagging/assigning_tags/?tab=containerizedenvironments#tags-cardinality">tags
 * cardinality documentation</a>.
 */
public enum TagsCardinality {
    /** Requests the cardinality the agent is configured to use for dogstatsd metrics. */
    DEFAULT(0x0000),

    /** Requests no origin tags at all. */
    NONE(0x1000),

    /** Requests low cardinality origin tags. */
    LOW(0x2000),

    /** Requests orchestrator cardinality origin tags. */
    ORCHESTRATOR(0x3000),

    /** Requests high cardinality origin tags. */
    HIGH(0x4000);

    private final int flag;

    TagsCardinality(int flag) {
        this.flag = flag;
    }

    int flag() {
        return flag;
    }
}
