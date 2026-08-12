/* Unless explicitly stated otherwise all files in this repository are
 * licensed under the Apache 2.0 License.
 *
 * This product includes software developed at Datadog
 *  (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.
 */

/** HTTP forwarder for DogStatsD metrics. */
module com.datadoghq.dogstatsd.http.forwarder {
    requires transitive com.datadoghq.dogstatsd.http;
    requires java.net.http;
    requires java.logging;

    exports com.datadoghq.dogstatsd.http.forwarder;
}
