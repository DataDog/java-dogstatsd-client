/* Unless explicitly stated otherwise all files in this repository are
 * licensed under the Apache 2.0 License.
 *
 * This product includes software developed at Datadog
 *  (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.
 */

package com.datadoghq.dogstatsd.http.serializer;

/** Values of the metricType nibble in the types column. */
final class MetricType {
    static final int COUNT = 1;
    static final int RATE = 2;
    static final int GAUGE = 3;
    static final int SKETCH = 4;
    static final int MERGEABLE_GAUGE = 8;
    static final int HISTOGRAM = 9;
    static final int SET = 10;

    private MetricType() {}
}
