package com.timgroup.statsd;

import java.io.IOException;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.comparesEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.startsWith;

public class AggregationTest {
    private UDPDummyStatsDServer server;
    private NonBlockingStatsDClient testClient;

    @Before
    public void start() throws IOException {
        server = new UDPDummyStatsDServer(0);
        testClient = new NonBlockingStatsDClientBuilder()
            .prefix("my.prefix")
            .hostname("localhost")
            .port(server.getPort())
            .enableTelemetry(true)
            .enableAggregation(true)
            .aggregationFlushInterval(100)
            // should be larger than aggregation interval to make sure test cases get clean
            // flush with only metrics they sent.
            .telemetryFlushInterval(500)
            .originDetectionEnabled(false)
            .build();
    }

    @After
    public void stop() throws IOException {
        server.close();
        testClient.stop();
    }

    @Test(timeout=5000L)
    public void testBasicGaugeAggregation() throws Exception {
        for (int i=0 ; i<10 ; i++) {
            testClient.gauge("top.level.value", i);
        }
        server.waitForMessage("my.prefix");

        List<String> messages = server.messagesReceived();

        assertThat(messages.size(), comparesEqualTo(1));
        assertThat(messages, hasItem(comparesEqualTo("my.prefix.top.level.value:9|g")));
    }

    @Test(timeout=5000L)
    public void testBasicCountAggregation() throws Exception {
        for (int i=0 ; i<10 ; i++) {
            testClient.count("top.level.count", i);
        }
        for (int i=0 ; i<10 ; i++) {
            testClient.increment("top.level.count");
        }

        server.waitForMessage("my.prefix");

        List<String> messages = server.messagesReceived();

        assertThat(messages.size(), comparesEqualTo(1));
        assertThat(messages, hasItem(comparesEqualTo("my.prefix.top.level.count:55|c")));
    }

    @Test(timeout=5000L)
    public void testSampledCountAggregation() throws Exception {
        for (int i=0 ; i<10 ; i++) {
            // NOTE: because aggregation is enabled, sampling is disabled, so all
            // counts should be accounted for, as if sample rate were 1.0.
            testClient.count("top.level.count", i, 0.1);
        }
        for (int i=0 ; i<10 ; i++) {
            testClient.increment("top.level.count");
        }

        server.waitForMessage("my.prefix");

        List<String> messages = server.messagesReceived();

        assertThat(messages.size(), comparesEqualTo(1));
        assertThat(messages, hasItem(comparesEqualTo("my.prefix.top.level.count:55|c")));
    }

    @Test(timeout=5000L)
    public void testBasicSetAggregation() throws Exception {
        for (int i=0 ; i<10 ; i++) {
            testClient.recordSetValue("top.level.set", "foo", null);
            testClient.recordSetValue("top.level.set", "bar", null);
        }

        server.waitForMessage("my.prefix");

        List<String> messages = server.messagesReceived();

        assertThat(messages.size(), comparesEqualTo(2));
        assertThat(messages, hasItem(comparesEqualTo("my.prefix.top.level.set:foo|s")));
        assertThat(messages, hasItem(comparesEqualTo("my.prefix.top.level.set:bar|s")));
    }

    @Test(timeout=5000L)
    public void testAggregationTelemetry() throws Exception {
        for (int i=0 ; i<10 ; i++) {
            testClient.gauge("top.level.value", i);
        }
        for (int i=0 ; i<10 ; i++) {
            testClient.count("top.level.count", i);
        }
        for (int i=0 ; i<10 ; i++) {
            testClient.increment("top.level.count.other");
        }

        server.waitForMessage("datadog");

        List<String> messages = server.messagesReceived();

        assertThat(messages.size(), comparesEqualTo(3+17));
        assertThat(messages, hasItem(startsWith("datadog.dogstatsd.client.aggregated_context:27|c")));
    }

    @Test(timeout=5000L)
    public void testBasicUnaggregatedMetrics() throws Exception {
        int submitted = 0;
        for (int i=0 ; i<10 ; i++) {
            testClient.histogram("top.level.hist", i);
            testClient.distribution("top.level.dist", i);
            testClient.time("top.level.time", i);
            submitted += 3;
        }
        server.waitForMessage("my.prefix");

        List<String> messages = server.messagesReceived();

        // there should be one message per
        assertThat(messages.size(), comparesEqualTo(submitted));
    }
}
