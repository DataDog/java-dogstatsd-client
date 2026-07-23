package com.timgroup.statsd;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;

public class NonBlockingStatsDClientBuilderTest {

    @Test(timeout = 5000L)
    public void origin_detection_env_false() throws Exception {
        final Map<String, String> env = new HashMap<>();
        env.put(NonBlockingStatsDClient.ORIGIN_DETECTION_ENABLED_ENV_VAR, "false");

        final NonBlockingStatsDClient client =
                new NonBlockingStatsDClientBuilder()
                        .withEnvironmentVariables(env)
                        .prefix("my.prefix")
                        .hostname("localhost")
                        .port(8125)
                        .queueSize(Integer.MAX_VALUE)
                        .errorHandler(null)
                        .enableTelemetry(false)
                        .build();

        assertFalse(
                client.isOriginDetectionEnabled(
                        NonBlockingStatsDClient.DEFAULT_ENABLE_ORIGIN_DETECTION));
    }

    @Test(timeout = 5000L)
    public void origin_detection_env_unknown() throws Exception {
        final Map<String, String> env = new HashMap<>();
        env.put(
                NonBlockingStatsDClient.ORIGIN_DETECTION_ENABLED_ENV_VAR,
                "unknown"); // default to true

        final NonBlockingStatsDClient client =
                new NonBlockingStatsDClientBuilder()
                        .withEnvironmentVariables(env)
                        .prefix("my.prefix")
                        .hostname("localhost")
                        .port(8125)
                        .queueSize(Integer.MAX_VALUE)
                        .errorHandler(null)
                        .enableAggregation(false)
                        .enableTelemetry(false)
                        .build();

        assertTrue(
                client.isOriginDetectionEnabled(
                        NonBlockingStatsDClient.DEFAULT_ENABLE_ORIGIN_DETECTION));
    }

    @Test(timeout = 5000L)
    public void origin_detection_env_unset() throws Exception {
        final Map<String, String> env = new HashMap<>();
        final NonBlockingStatsDClient client =
                new NonBlockingStatsDClientBuilder()
                        .withEnvironmentVariables(env)
                        .prefix("my.prefix")
                        .hostname("localhost")
                        .port(8125)
                        .queueSize(Integer.MAX_VALUE)
                        .errorHandler(null)
                        .enableAggregation(false)
                        .enableTelemetry(false)
                        .build();

        assertTrue(
                client.isOriginDetectionEnabled(
                        NonBlockingStatsDClient.DEFAULT_ENABLE_ORIGIN_DETECTION));
    }

    @Test(timeout = 5000L)
    public void origin_detection_arg_false() throws Exception {
        final Map<String, String> env = new HashMap<>();
        final NonBlockingStatsDClient client =
                new NonBlockingStatsDClientBuilder()
                        .withEnvironmentVariables(env)
                        .prefix("my.prefix")
                        .hostname("localhost")
                        .port(8125)
                        .queueSize(Integer.MAX_VALUE)
                        .errorHandler(null)
                        .enableTelemetry(false)
                        .build();

        assertFalse(client.isOriginDetectionEnabled(false));
    }

    @Test(timeout = 5000L)
    public void address_resolution_empty() throws Exception {
        assertThrows(
                StatsDClientException.class,
                new ThrowingRunnable() {
                    @Override
                    public void run() {
                        new NonBlockingStatsDClientBuilder()
                                .withEnvironmentVariables(new HashMap<String, String>())
                                .resolve();
                    }
                });
    }

    @Test
    public void tags_cardinality() throws Exception {
        final Map<String, String> env = new HashMap<>();
        env.put("DD_DOGSTATSD_URL", "localhost:8125");
        // default value
        assertEquals(
                TagsCardinality.DEFAULT,
                new NonBlockingStatsDClientBuilder()
                        .withEnvironmentVariables(env)
                        .resolve()
                        .tagsCardinality);
        // one env variable works
        env.put("DATADOG_CARDINALITY", "low");
        assertEquals(
                TagsCardinality.LOW,
                new NonBlockingStatsDClientBuilder()
                        .withEnvironmentVariables(env)
                        .resolve()
                        .tagsCardinality);
        // the other variable takes precedence
        env.put("DD_CARDINALITY", "high");
        assertEquals(
                TagsCardinality.HIGH,
                new NonBlockingStatsDClientBuilder()
                        .withEnvironmentVariables(env)
                        .resolve()
                        .tagsCardinality);
        // explicit user input takes precedence even if they request default
        assertEquals(
                TagsCardinality.DEFAULT,
                new NonBlockingStatsDClientBuilder()
                        .withEnvironmentVariables(env)
                        .tagsCardinality(TagsCardinality.DEFAULT)
                        .resolve()
                        .tagsCardinality);
    }
}
