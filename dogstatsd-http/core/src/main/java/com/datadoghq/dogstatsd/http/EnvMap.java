/* Unless explicitly stated otherwise all files in this repository are
 * licensed under the Apache 2.0 License.
 *
 * This product includes software developed at Datadog
 *  (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.
 */

package com.datadoghq.dogstatsd.http;

import java.util.Map;

class EnvMap {
    private final Map<String, String> env;

    EnvMap() {
        env = null;
    }

    EnvMap(Map<String, String> provided) {
        env = provided;
    }

    String get(String name) {
        return env != null ? env.get(name) : System.getenv(name);
    }
}
