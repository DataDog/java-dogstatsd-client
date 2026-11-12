package com.timgroup.statsd;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.comparesEqualTo;
import static org.hamcrest.Matchers.hasItem;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class NonBlockingDirectStatsDClientTest {

    private static final int STATSD_SERVER_PORT = 17256;
    private static final int MAX_PACKET_SIZE = 64;
    private static DirectStatsDClient client;
    private static DummyStatsDServer server;

    // Controlled (empty) environment so the client is unaffected by ambient DD_* variables.
    private static final Map<String, String> CLIENT_ENV = new HashMap<>();

    @BeforeClass
    public static void start() throws IOException {
        server = new UDPDummyStatsDServer(STATSD_SERVER_PORT);
        client =
                new NonBlockingStatsDClientBuilder()
                        .withEnvironmentVariables(CLIENT_ENV)
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
        client.recordDistributionValues("mydistribution", new long[] {423L, 234L}, Double.NaN);
        server.waitForMessage("my.prefix");

        assertThat(
                server.messagesReceived(),
                hasItem(comparesEqualTo("my.prefix.mydistribution:423:234|d")));
    }

    @Test(timeout = 5000L)
    public void sends_double_multivalued_distribution_to_statsd() {
        client.recordDistributionValues(
                "mydistribution", new double[] {0.423D, 0.234D}, Double.NaN);
        server.waitForMessage("my.prefix");

        assertThat(
                server.messagesReceived(),
                hasItem(comparesEqualTo("my.prefix.mydistribution:0.423:0.234|d")));
    }

    @Test(timeout = 5000L)
    public void sends_multivalued_distribution_to_statsd_with_tags() {
        client.recordDistributionValues(
                "mydistribution", new long[] {423L, 234L}, Double.NaN, "foo:bar", "baz");
        server.waitForMessage("my.prefix");

        assertThat(
                server.messagesReceived(),
                hasItem(comparesEqualTo("my.prefix.mydistribution:423:234|d|#baz,foo:bar")));
    }

    @Test(timeout = 5000L)
    public void sends_multivalued_distribution_to_statsd_with_sampling_rate() {
        client.recordDistributionValues("mydistribution", new long[] {423L, 234L}, 1);
        server.waitForMessage("my.prefix");

        assertThat(
                server.messagesReceived(),
                hasItem(comparesEqualTo("my.prefix.mydistribution:423:234|d|@1.000000")));
    }

    @Test(timeout = 5000L)
    public void sends_multivalued_distribution_to_statsd_with_non_1_sampling_rate() {
        client.recordDistributionValues("mydistribution", new long[] {423L, 234L}, 0.1);
        server.waitForMessage("my.prefix");

        assertThat(
                server.messagesReceived(),
                hasItem(comparesEqualTo("my.prefix.mydistribution:423:234|d|@0.100000")));
    }

    @Test(timeout = 5000L)
    public void sends_multivalued_distribution_to_statsd_with_tags_and_sampling_rate() {
        client.recordDistributionValues(
                "mydistribution", new long[] {423L, 234L}, 1, "foo:bar", "baz");
        server.waitForMessage("my.prefix");

        assertThat(
                server.messagesReceived(),
                hasItem(
                        comparesEqualTo(
                                "my.prefix.mydistribution:423:234|d|@1.000000|#baz,foo:bar")));
    }

    @Test(timeout = 5000L)
    public void sends_too_long_multivalued_distribution_to_statsd() {
        long[] values = {423L, 234L, 456L, 512L, 345L, 898L, 959876543123L, 667L};
        client.recordDistributionValues("mydistribution", values, 0.4, "foo:bar", "baz");

        server.waitForMessage("my.prefix");
        assertThat(
                server.messagesReceived(),
                hasItem(
                        comparesEqualTo(
                                "my.prefix.mydistribution:423:234:456|d|@0.400000|#baz,foo:bar")));

        server.waitForMessage("my.prefix");
        assertThat(
                server.messagesReceived(),
                hasItem(
                        comparesEqualTo(
                                "my.prefix.mydistribution:512:345:898|d|@0.400000|#baz,foo:bar")));

        server.waitForMessage("my.prefix");
        assertThat(
                server.messagesReceived(),
                hasItem(
                        comparesEqualTo(
                                "my.prefix.mydistribution:959876543123|d|@0.400000|#baz,foo:bar")));

        server.waitForMessage("my.prefix");
        assertThat(
                server.messagesReceived(),
                hasItem(comparesEqualTo("my.prefix.mydistribution:667|d|@0.400000|#baz,foo:bar")));
    }

    @Test(timeout = 5000L)
    public void sends_multivalued_distribution_chunk_that_exactly_fills_the_packet()
            throws Exception {
        // The first chunk, including its trailing newline, is exactly MAX_PACKET_SIZE bytes.
        String first = "my.prefix.mydistribution:4234:2345:678|d|@0.400000|#baz,foo:bar";
        String second = "my.prefix.mydistribution:1|d|@0.400000|#baz,foo:bar";
        assertThat(first.length() + 1, comparesEqualTo(MAX_PACKET_SIZE));

        RecordingErrorHandler errors = new RecordingErrorHandler();
        DirectStatsDClient exactFitClient = buildClient(errors);
        try {
            exactFitClient.recordDistributionValues(
                    "mydistribution", new long[] {4234L, 2345L, 678L, 1L}, 0.4, "foo:bar", "baz");
            waitForMessages(errors, first, second);
        } finally {
            exactFitClient.stop();
        }
    }

    @Test(timeout = 5000L)
    public void sends_message_that_exactly_fills_the_packet() throws Exception {
        String prefixAndSuffix = "my.prefix.:1|d\n";
        char[] aspect = new char[MAX_PACKET_SIZE - prefixAndSuffix.length()];
        Arrays.fill(aspect, 'x');
        String expected = "my.prefix." + new String(aspect) + ":1|d";

        RecordingErrorHandler errors = new RecordingErrorHandler();
        DirectStatsDClient exactFitClient = buildClient(errors);
        try {
            exactFitClient.distribution(new String(aspect), 1L);
            waitForMessages(errors, expected);
        } finally {
            exactFitClient.stop();
        }
    }

    private static DirectStatsDClient buildClient(StatsDClientErrorHandler errorHandler) {
        return new NonBlockingStatsDClientBuilder()
                .withEnvironmentVariables(CLIENT_ENV)
                .prefix("my.prefix")
                .hostname("localhost")
                .port(STATSD_SERVER_PORT)
                .enableTelemetry(false)
                .originDetectionEnabled(false)
                .maxPacketSizeBytes(MAX_PACKET_SIZE)
                .errorHandler(errorHandler)
                .buildDirectStatsDClient();
    }

    private static void waitForMessages(RecordingErrorHandler errors, String... expected)
            throws InterruptedException {
        List<String> expectedMessages = Arrays.asList(expected);
        while (!server.messagesReceived().containsAll(expectedMessages)) {
            List<Exception> exceptions = errors.getExceptions();
            if (!exceptions.isEmpty()) {
                throw new AssertionError("client reported errors: " + exceptions);
            }
            Thread.sleep(10L);
        }
    }
}
