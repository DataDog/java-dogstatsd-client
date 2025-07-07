package com.timgroup.statsd;

import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.function.ThrowingRunnable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class NonBlockingStatsDClientBuilderTest {
    @Rule
    public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

    @Test(timeout = 5000L)
    public void origin_detection_env_false() throws Exception {
        environmentVariables.set(NonBlockingStatsDClient.ORIGIN_DETECTION_ENABLED_ENV_VAR, "false");

        final NonBlockingStatsDClient client = new NonBlockingStatsDClientBuilder()
            .prefix("my.prefix")
            .hostname("localhost")
            .port(8125)
            .queueSize(Integer.MAX_VALUE)
            .errorHandler(null)
            .enableTelemetry(false)
            .build();

        assertFalse(client.isOriginDetectionEnabled(NonBlockingStatsDClient.DEFAULT_ENABLE_ORIGIN_DETECTION));
        environmentVariables.clear(NonBlockingStatsDClient.ORIGIN_DETECTION_ENABLED_ENV_VAR);
    }

    @Test(timeout = 5000L)
    public void origin_detection_env_unknown() throws Exception {
        environmentVariables.set(NonBlockingStatsDClient.ORIGIN_DETECTION_ENABLED_ENV_VAR, "unknown"); // default to true

        final NonBlockingStatsDClient client = new NonBlockingStatsDClientBuilder()
            .prefix("my.prefix")
            .hostname("localhost")
            .port(8125)
            .queueSize(Integer.MAX_VALUE)
            .errorHandler(null)
            .enableAggregation(false)
            .enableTelemetry(false)
            .build();

        assertTrue(client.isOriginDetectionEnabled(NonBlockingStatsDClient.DEFAULT_ENABLE_ORIGIN_DETECTION));
        environmentVariables.clear(NonBlockingStatsDClient.ORIGIN_DETECTION_ENABLED_ENV_VAR);
    }

    @Test(timeout = 5000L)
    public void origin_detection_env_unset() throws Exception {
        final NonBlockingStatsDClient client = new NonBlockingStatsDClientBuilder()
            .prefix("my.prefix")
            .hostname("localhost")
            .port(8125)
            .queueSize(Integer.MAX_VALUE)
            .errorHandler(null)
            .enableAggregation(false)
            .enableTelemetry(false)
            .build();

        assertTrue(client.isOriginDetectionEnabled(NonBlockingStatsDClient.DEFAULT_ENABLE_ORIGIN_DETECTION));
    }

    @Test(timeout = 5000L)
    public void origin_detection_arg_false() throws Exception {
        final NonBlockingStatsDClient client = new NonBlockingStatsDClientBuilder()
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
        assertThrows(StatsDClientException.class, new ThrowingRunnable() {
                @Override
                public void run() {
                    new NonBlockingStatsDClientBuilder().resolve();
                }
            });
    }

    @Test
    public void tags_cardinality() throws Exception {
        environmentVariables.set("DD_DOGSTATSD_URL", "localhost:8125");
        // default value
        assertEquals(TagsCardinality.DEFAULT,
            new NonBlockingStatsDClientBuilder().resolve().tagsCardinality);
        // one env variable works
        environmentVariables.set("DATADOG_CARDINALITY", "low");
        assertEquals(TagsCardinality.LOW,
            new NonBlockingStatsDClientBuilder().resolve().tagsCardinality);
        // the other variable takes precedence
        environmentVariables.set("DD_CARDINALITY", "high");
        assertEquals(TagsCardinality.HIGH,
            new NonBlockingStatsDClientBuilder().resolve().tagsCardinality);
        // explicit user input takes precedence even if they request default
        assertEquals(TagsCardinality.DEFAULT,
            new NonBlockingStatsDClientBuilder()
                .tagsCardinality(TagsCardinality.DEFAULT)
                .resolve().tagsCardinality);
    }
}
