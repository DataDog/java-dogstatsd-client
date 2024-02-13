package com.timgroup.statsd;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.runners.MethodSorters;

import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.comparesEqualTo;
import static org.hamcrest.Matchers.hasItem;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class NonBlockingDirectStatsDClientTest {

    private static final int STATSD_SERVER_PORT = 17256;
    private static final int MAX_PACKET_SIZE = 64;
    private static DirectStatsDClient client;
    private static DummyStatsDServer server;

    @Rule
    public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

    @BeforeClass
    public static void start() throws IOException {
        server = new UDPDummyStatsDServer(STATSD_SERVER_PORT);
        client = new NonBlockingStatsDClientBuilder()
                .prefix("my.prefix")
                .hostname("localhost")
                .port(STATSD_SERVER_PORT)
                .enableTelemetry(false)
                .originDetectionEnabled(false)
                .maxPacketSizeBytes(MAX_PACKET_SIZE)
                .buildDirectStatsDClient();
    }

    @AfterClass
    public static void stop() {
        try {
            client.stop();
            server.close();
        } catch (java.io.IOException ignored) {
        }
    }

    @After
    public void clear() {
        server.clear();
    }


    @Test(timeout = 5000L)
    public void sends_multivalued_distribution_to_statsd() {
        client.recordDistributionValues("mydistribution", new long[] { 423L, 234L }, Double.NaN);
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mydistribution:423:234|d")));
    }

    @Test(timeout = 5000L)
    public void sends_double_multivalued_distribution_to_statsd() {
        client.recordDistributionValues("mydistribution", new double[] { 0.423D, 0.234D }, Double.NaN);
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mydistribution:0.423:0.234|d")));
    }

    @Test(timeout = 5000L)
    public void sends_multivalued_distribution_to_statsd_with_tags() {
        client.recordDistributionValues("mydistribution", new long[] { 423L, 234L }, Double.NaN, "foo:bar", "baz");
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mydistribution:423:234|d|#baz,foo:bar")));
    }

    @Test(timeout = 5000L)
    public void sends_multivalued_distribution_to_statsd_with_sampling_rate() {
        client.recordDistributionValues("mydistribution", new long[] { 423L, 234L }, 1);
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mydistribution:423:234|d|@1.000000")));
    }

    @Test(timeout = 5000L)
    public void sends_multivalued_distribution_to_statsd_with_tags_and_sampling_rate() {
        client.recordDistributionValues("mydistribution", new long[] { 423L, 234L }, 1, "foo:bar", "baz");
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mydistribution:423:234|d|@1.000000|#baz,foo:bar")));
    }

    @Test(timeout = 5000L)
    public void sends_too_long_multivalued_distribution_to_statsd() {
        long[] values = {423L, 234L, 456L, 512L, 345L, 898L, 959876543123L, 667L};
        client.recordDistributionValues("mydistribution", values, 1, "foo:bar", "baz");

        server.waitForMessage("my.prefix");
        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mydistribution:423:234:456|d|@1.000000|#baz,foo:bar")));

        server.waitForMessage("my.prefix");
        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mydistribution:512:345:898|d|@1.000000|#baz,foo:bar")));

        server.waitForMessage("my.prefix");
        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mydistribution:959876543123|d|@1.000000|#baz,foo:bar")));

        server.waitForMessage("my.prefix");
        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mydistribution:667|d|@1.000000|#baz,foo:bar")));
    }

}
