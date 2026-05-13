/* Unless explicitly stated otherwise all files in this repository are
 * licensed under the Apache 2.0 License.
 *
 * This product includes software developed at Datadog
 *  (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.
 */

package com.datadoghq.dogstatsd.http.forwarder;

/** Canonical telemetry labels for HTTP response codes. */
final class HttpCode {

    private static final String[] CATEGORIES = {"0xx", "1xx", "2xx", "3xx", "4xx", "5xx"};

    private static final String[] NAMES = new String[506];

    static {
        for (int i = 0; i < NAMES.length; i++) {
            NAMES[i] = CATEGORIES[i / 100];
        }
        NAMES[0] = "0";
        NAMES[200] = "200";
        NAMES[400] = "400";
        NAMES[403] = "403";
        NAMES[404] = "404";
        NAMES[429] = "429";
        NAMES[500] = "500";
        NAMES[501] = "501";
        NAMES[502] = "502";
        NAMES[503] = "503";
        NAMES[504] = "504";
        NAMES[505] = "505";
    }

    private HttpCode() {}

    static String name(int code) {
        if (code >= 0 && code < NAMES.length) {
            return NAMES[code];
        }
        int category = code / 100;
        if (category >= 0 && category < CATEGORIES.length) {
            return CATEGORIES[category];
        }
        return "xxx";
    }
}
