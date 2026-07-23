package com.timgroup.statsd;

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
