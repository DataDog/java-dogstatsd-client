package com.timgroup.statsd;


import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Rule;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;
import org.junit.function.ThrowingRunnable;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;
import java.text.NumberFormat;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.comparesEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertThrows;

import org.junit.function.ThrowingRunnable;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class NonBlockingStatsDClientTest {

    private static final int STATSD_SERVER_PORT = 17254;
    private static NonBlockingStatsDClient client;
    private static NonBlockingStatsDClient clientUnaggregated;
    private static NonBlockingStatsDClient clientWithContainerID;
    private static DummyStatsDServer server;

    private static Logger log = Logger.getLogger("NonBlockingStatsDClientTest");

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
            .build();
        clientUnaggregated = new NonBlockingStatsDClientBuilder()
            .prefix("my.prefix")
            .hostname("localhost")
            .port(STATSD_SERVER_PORT)
            .enableTelemetry(false)
            .enableAggregation(false)
            .originDetectionEnabled(false)
            .build();
        clientWithContainerID = new NonBlockingStatsDClientBuilder()
            .prefix("my.prefix")
            .hostname("localhost")
            .port(STATSD_SERVER_PORT)
            .enableTelemetry(false)
            .containerID("fake-container-id")
            .build();
    }

    @AfterClass
    public static void stop() {
        try {
            client.stop();
            clientUnaggregated.stop();
            clientWithContainerID.stop();
            server.close();
        } catch (java.io.IOException ignored) {
        }
    }

    @After
    public void clear() {
        server.clear();
    }

    @Test
    public void assert_default_udp_size() throws Exception {
        assertEquals(client.statsDProcessor.bufferPool.getBufferSize(), NonBlockingStatsDClient.DEFAULT_UDP_MAX_PACKET_SIZE_BYTES);
    }

    @Test(timeout = 5000L)
    public void sends_counter_value_to_statsd() throws Exception {

        client.count("mycount", 24);
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mycount:24|c")));
    }

    @Test(timeout = 5000L)
    public void sends_counter_value_with_sample_rate_to_statsd() throws Exception {

        clientUnaggregated.count("mycount", 24, 1);
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mycount:24|c|@1.000000")));
    }

    @Test(timeout = 5000L)
    public void sends_counter_value_to_statsd_with_null_tags() throws Exception {

        client.count("mycount", 24, (java.lang.String[]) null);
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mycount:24|c")));
    }

    @Test(timeout = 5000L)
    public void sends_counter_value_to_statsd_with_empty_tags() throws Exception {

        client.count("mycount", 24);
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mycount:24|c")));
    }

    @Test(timeout = 5000L)
    public void sends_counter_value_to_statsd_with_tags() throws Exception {

        client.count("mycount", 24, "foo:bar", "baz");
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mycount:24|c|#baz,foo:bar")));
    }

    @Test(timeout = 5000L)
    public void sends_counter_value_with_sample_rate_to_statsd_with_tags() throws Exception {

        clientUnaggregated.count("mycount", 24, 1, "foo:bar", "baz");
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mycount:24|c|@1.000000|#baz,foo:bar")));
    }

    @Test(timeout = 5000L)
    public void sends_long_counter_value_with_timestamp() throws Exception {
        clientUnaggregated.countWithTimestamp("mycount", 24l, 1032127200, "foo:bar", "baz");
        clientUnaggregated.countWithTimestamp("mycount", 42l, 1032127200, "foo:bar", "baz");
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mycount:24|c|T1032127200|#baz,foo:bar")));
        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mycount:42|c|T1032127200|#baz,foo:bar")));
    }

    @Test(timeout = 5000L)
    public void sends_double_counter_value_with_timestamp() throws Exception {
        clientUnaggregated.countWithTimestamp("mycount", 24.5d, 1032127200, "foo:bar", "baz");
        clientUnaggregated.countWithTimestamp("mycount", 42.5d, 1032127200, "foo:bar", "baz");
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mycount:24.5|c|T1032127200|#baz,foo:bar")));
        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mycount:42.5|c|T1032127200|#baz,foo:bar")));
    }

    @Test(timeout = 5000L)
    public void sends_long_counter_value_with_incorrect_timestamp() throws Exception {
        clientUnaggregated.countWithTimestamp("mycount", 24l, -1, "foo:bar", "baz");
        clientUnaggregated.countWithTimestamp("mycount", 42l, 0, "foo:bar", "baz");
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mycount:24|c|T1|#baz,foo:bar")));
        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mycount:42|c|T1|#baz,foo:bar")));
    }

    @Test(timeout = 5000L)
    public void sends_double_counter_value_with_incorrect_timestamp() throws Exception {
        clientUnaggregated.countWithTimestamp("mycount", 24.5d, -1, "foo:bar", "baz");
        clientUnaggregated.countWithTimestamp("mycount", 42.5d, 0, "foo:bar", "baz");
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mycount:24.5|c|T1|#baz,foo:bar")));
        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mycount:42.5|c|T1|#baz,foo:bar")));
    }

    @Test(timeout = 5000L)
    public void sends_counter_increment_to_statsd() throws Exception {

        client.incrementCounter("myinc");
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.myinc:1|c")));
    }

    @Test(timeout = 5000L)
    public void sends_counter_increment_to_statsd_with_tags() {

        client.incrementCounter("myinc", "foo:bar", "baz");
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.myinc:1|c|#baz,foo:bar")));
    }

    @Test(timeout = 5000L)
    public void sends_counter_increment_with_sample_rate_to_statsd_with_tags() throws Exception {

        clientUnaggregated.incrementCounter("myinc", 1, "foo:bar", "baz");
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.myinc:1|c|@1.000000|#baz,foo:bar")));
    }

    @Test(timeout = 5000L)
    public void sends_counter_decrement_to_statsd() throws Exception {

        client.decrementCounter("mydec");
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mydec:-1|c")));
    }

    @Test(timeout = 5000L)
    public void sends_counter_decrement_to_statsd_with_tags() throws Exception {

        client.decrementCounter("mydec", "foo:bar", "baz");
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mydec:-1|c|#baz,foo:bar")));
    }

    @Test(timeout = 5000L)
    public void sends_counter_decrement_with_sample_rate_to_statsd_with_tags() throws Exception {

        clientUnaggregated.decrementCounter("mydec", 1, "foo:bar", "baz");
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mydec:-1|c|@1.000000|#baz,foo:bar")));
    }

    @Test(timeout = 5000L)
    public void sends_gauge_to_statsd() throws Exception {


        client.recordGaugeValue("mygauge", 423);
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mygauge:423|g")));
    }

    // Test that we don't leave partially encoded message in the
    // buffer on overflow, when a message is too big to be encoded
    // successfully.
    @Test(timeout = 5000L)
    public void sends_gauge_without_truncation() throws Exception {
        final RecordingErrorHandler errorHandler = new RecordingErrorHandler();
        final NonBlockingStatsDClient client = new NonBlockingStatsDClientBuilder()
            .prefix("")
            .hostname("localhost")
            .port(STATSD_SERVER_PORT)
            .errorHandler(errorHandler)
            .originDetectionEnabled(false)
            .build();

        try {
            ArrayList<String> tags = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                tags.add("🏷️🏷️🏷️🏷️");
            }

            client.recordGaugeValue("mygauge", 423);
            client.recordGaugeValue("badgauge", 1, tags.toArray(new String[]{}));
            client.recordGaugeValue("marker", 1);

            server.waitForMessage("marker");

            assertThat(errorHandler.getExceptions(), hasSize(1));
            // Shouldn't be InvalidMessageException, which would mean we filtered the payload before encoding.
            assertThat(errorHandler.getExceptions(), hasItem(isA(BufferOverflowException.class)));

            assertThat(server.messagesReceived(), hasSize(2));
            assertThat(server.messagesReceived(), hasItem(comparesEqualTo("mygauge:423|g")));
            assertThat(server.messagesReceived(), hasItem(comparesEqualTo("marker:1|g")));
            assertThat(server.messagesReceived(), not(hasItem(startsWith("badgauge"))));
        } finally {
            client.stop();
        }
    }

    @Test(timeout = 5000L)
    public void sends_gauge_with_sample_rate_to_statsd() throws Exception {

        clientUnaggregated.recordGaugeValue("mygauge", 423, 1);
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mygauge:423|g|@1.000000")));
    }

    @Test(timeout = 5000L)
    public void sends_large_double_gauge_to_statsd() throws Exception {


        client.recordGaugeValue("mygauge", 123456789012345.67890);
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mygauge:123456789012345.67|g")));
    }

    @Test(timeout = 5000L)
    public void sends_exact_double_gauge_to_statsd() throws Exception {


        client.recordGaugeValue("mygauge", 123.45678901234567890);
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mygauge:123.456789|g")));
    }

    @Test(timeout = 5000L)
    public void sends_double_gauge_to_statsd() throws Exception {


        client.recordGaugeValue("mygauge", 0.423);
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mygauge:0.423|g")));
    }

    @Test(timeout = 5000L)
    public void sends_gauge_to_statsd_with_tags() throws Exception {


        client.recordGaugeValue("mygauge", 423, "foo:bar", "baz");
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mygauge:423|g|#baz,foo:bar")));
    }

    @Test(timeout = 5000L)
    public void sends_gauge_with_sample_rate_to_statsd_with_tags() throws Exception {

        clientUnaggregated.recordGaugeValue("mygauge", 423, 1, "foo:bar", "baz");
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mygauge:423|g|@1.000000|#baz,foo:bar")));
    }

    @Test(timeout = 5000L)
    public void sends_long_gauge_with_timestamp() throws Exception {
        clientUnaggregated.gaugeWithTimestamp("mygauge", 234l, 1205794800, "foo:bar", "baz");
        clientUnaggregated.gaugeWithTimestamp("mygauge", 423l, 1205794800, "foo:bar", "baz");
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mygauge:234|g|T1205794800|#baz,foo:bar")));
        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mygauge:423|g|T1205794800|#baz,foo:bar")));
    }

    @Test(timeout = 5000L)
    public void sends_double_gauge_with_timestamp() throws Exception {
        clientUnaggregated.gaugeWithTimestamp("mygauge", 243.5d, 1205794800, "foo:bar", "baz");
        clientUnaggregated.gaugeWithTimestamp("mygauge", 423.5d, 1205794800, "foo:bar", "baz");
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mygauge:243.5|g|T1205794800|#baz,foo:bar")));
        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mygauge:423.5|g|T1205794800|#baz,foo:bar")));
    }

    @Test(timeout = 5000L)
    public void sends_long_gauge_with_incorrect_timestamp() throws Exception {
        clientUnaggregated.gaugeWithTimestamp("mygauge", 234l, 0, "foo:bar", "baz");
        clientUnaggregated.gaugeWithTimestamp("mygauge", 423l, -1, "foo:bar", "baz");
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mygauge:234|g|T1|#baz,foo:bar")));
        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mygauge:423|g|T1|#baz,foo:bar")));
    }

    @Test(timeout = 5000L)
    public void sends_double_gauge_with_incorrect_timestamp() throws Exception {
        clientUnaggregated.gaugeWithTimestamp("mygauge", 243.5d, 0, "foo:bar", "baz");
        clientUnaggregated.gaugeWithTimestamp("mygauge", 423.5d, -1, "foo:bar", "baz");
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mygauge:243.5|g|T1|#baz,foo:bar")));
        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mygauge:423.5|g|T1|#baz,foo:bar")));
    }

    @Test(timeout = 5000L)
    public void sends_double_gauge_to_statsd_with_tags() throws Exception {


        client.recordGaugeValue("mygauge", 0.423, "foo:bar", "baz");
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mygauge:0.423|g|#baz,foo:bar")));
    }

    @Test(timeout = 5000L)
    public void sends_histogram_to_statsd() throws Exception {

        client.recordHistogramValue("myhistogram", 423);
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.myhistogram:423|h")));
    }

    @Test(timeout = 5000L)
    public void sends_double_histogram_to_statsd() throws Exception {


        client.recordHistogramValue("myhistogram", 0.423);
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.myhistogram:0.423|h")));
    }

    @Test(timeout = 5000L)
    public void sends_histogram_to_statsd_with_tags() throws Exception {


        client.recordHistogramValue("myhistogram", 423, "foo:bar", "baz");
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.myhistogram:423|h|#baz,foo:bar")));
    }

    @Test(timeout = 5000L)
    public void sends_histogram_with_sample_rate_to_statsd_with_tags() throws Exception {

        clientUnaggregated.recordHistogramValue("myhistogram", 423, 1, "foo:bar", "baz");
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.myhistogram:423|h|@1.000000|#baz,foo:bar")));
    }

    @Test(timeout = 5000L)
    public void sends_double_histogram_to_statsd_with_tags() throws Exception {


        client.recordHistogramValue("myhistogram", 0.423, "foo:bar", "baz");
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.myhistogram:0.423|h|#baz,foo:bar")));
    }

    @Test(timeout = 5000L)
    public void sends_double_histogram_with_sample_rate_to_statsd_with_tags() throws Exception {

        clientUnaggregated.recordHistogramValue("myhistogram", 0.423, 1, "foo:bar", "baz");
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.myhistogram:0.423|h|@1.000000|#baz,foo:bar")));
    }

    @Test(timeout = 5000L)
    public void sends_distribtuion_to_statsd() throws Exception {

        client.recordDistributionValue("mydistribution", 423);
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mydistribution:423|d")));
    }

    @Test(timeout = 5000L)
    public void sends_double_distribution_to_statsd() throws Exception {


        client.recordDistributionValue("mydistribution", 0.423);
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mydistribution:0.423|d")));
    }

    @Test(timeout = 5000L)
    public void sends_distribution_to_statsd_with_tags() throws Exception {


        client.recordDistributionValue("mydistribution", 423, "foo:bar", "baz");
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mydistribution:423|d|#baz,foo:bar")));
    }

    @Test(timeout = 5000L)
    public void sends_distribution_with_sample_rate_to_statsd_with_tags() throws Exception {

        clientUnaggregated.recordDistributionValue("mydistribution", 423, 1, "foo:bar", "baz");
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mydistribution:423|d|@1.000000|#baz,foo:bar")));
    }

    @Test(timeout = 5000L)
    public void sends_double_distribution_to_statsd_with_tags() throws Exception {


        client.recordDistributionValue("mydistribution", 0.423, "foo:bar", "baz");
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mydistribution:0.423|d|#baz,foo:bar")));
    }

    @Test(timeout = 5000L)
    public void sends_double_distribution_with_sample_rate_to_statsd_with_tags() throws Exception {

        clientUnaggregated.recordDistributionValue("mydistribution", 0.423, 1, "foo:bar", "baz");
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mydistribution:0.423|d|@1.000000|#baz,foo:bar")));
    }

    @Test(timeout = 5000L)
    public void sends_timer_to_statsd() throws Exception {


        client.recordExecutionTime("mytime", 123);
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mytime:123|ms")));
    }

    /**
     * A regression test for <a href="https://github.com/indeedeng/java-dogstatsd-client/issues/3">this i18n number formatting bug</a>
     *
     * @throws Exception
     */
    @Test
    public void
    sends_timer_to_statsd_from_locale_with_unamerican_number_formatting() throws Exception {

        Locale originalDefaultLocale = Locale.getDefault();

        // change the default Locale to one that uses something other than a '.' as the decimal separator (Germany uses a comma)
        Locale.setDefault(Locale.GERMANY);

        try {

            client.recordExecutionTime("mytime", 123, "foo:bar", "baz");
            server.waitForMessage("my.prefix");

            assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mytime:123|ms|#baz,foo:bar")));
        } finally {
            // reset the default Locale in case changing it has side-effects
            Locale.setDefault(originalDefaultLocale);
        }
    }


    @Test(timeout = 5000L)
    public void sends_timer_to_statsd_with_tags() throws Exception {

        client.recordExecutionTime("mytime", 123, "foo:bar", "baz");
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mytime:123|ms|#baz,foo:bar")));
    }

    @Test(timeout = 5000L)
    public void sends_timer_with_sample_rate_to_statsd_with_tags() throws Exception {

        clientUnaggregated.recordExecutionTime("mytime", 123, 1, "foo:bar", "baz");
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mytime:123|ms|@1.000000|#baz,foo:bar")));
    }

    @Test(timeout = 5000L)
    public void sends_gauge_mixed_tags_deprecated() throws Exception {

        final NonBlockingStatsDClient client = new NonBlockingStatsDClientBuilder()
            .prefix("my.prefix")
            .hostname("localhost")
            .port(STATSD_SERVER_PORT)
            .queueSize(Integer.MAX_VALUE)
            .constantTags("instance:foo", "app:bar")
            .originDetectionEnabled(false)
            .build();
        try {
            client.gauge("value", 423, "baz");
            server.waitForMessage("my.prefix");

            assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.value:423|g|#app:bar,instance:foo,baz")));
        } finally {
            client.stop();
        }
    }

    @Test(timeout = 5000L)
    public void sends_gauge_mixed_tags() throws Exception {

        final NonBlockingStatsDClient client = new NonBlockingStatsDClientBuilder()
            .prefix("my.prefix")
            .hostname("localhost")
            .port(STATSD_SERVER_PORT)
            .queueSize(Integer.MAX_VALUE)
            .constantTags("instance:foo", "app:bar")
            .originDetectionEnabled(false)
            .build();
        try {
            client.gauge("value", 423, "baz");
            server.waitForMessage("my.prefix");

            assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.value:423|g|#app:bar,instance:foo,baz")));
        } finally {
            client.stop();
        }
    }

    @Test(timeout = 5000L)
    public void sends_gauge_mixed_tags_with_sample_rate_deprecated() throws Exception {

        final NonBlockingStatsDClient client = new NonBlockingStatsDClientBuilder()
            .prefix("my.prefix")
            .hostname("localhost")
            .port(STATSD_SERVER_PORT)
            .queueSize(Integer.MAX_VALUE)
            .constantTags("instance:foo", "app:bar")
            .originDetectionEnabled(false)
            .build();
        try {
            client.gauge("value", 423, 1, "baz");
            server.waitForMessage("my.prefix.value:423");

            List<String> messages = server.messagesReceived();
            assertThat(messages, hasItem(comparesEqualTo("my.prefix.value:423|g|@1.000000|#app:bar,instance:foo,baz")));
        } finally {
            client.stop();
        }
    }

    @Test(timeout = 5000L)
    public void sends_gauge_mixed_tags_with_sample_rate() throws Exception {

        final NonBlockingStatsDClient client = new NonBlockingStatsDClientBuilder()
            .prefix("my.prefix")
            .hostname("localhost")
            .port(STATSD_SERVER_PORT)
            .queueSize(Integer.MAX_VALUE)
            .constantTags("instance:foo", "app:bar")
            .enableAggregation(false)
            .originDetectionEnabled(false)
            .build();
        try {
            client.gauge("value", 423, 1, "baz");
            server.waitForMessage("my.prefix");

            assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.value:423|g|@1.000000|#app:bar,instance:foo,baz")));
        } finally {
            client.stop();
        }
    }

    @Test(timeout = 5000L)
    public void sends_gauge_constant_tags_only_deprecated() throws Exception {

        final NonBlockingStatsDClient client = new NonBlockingStatsDClientBuilder()
            .prefix("my.prefix")
            .hostname("localhost")
            .port(STATSD_SERVER_PORT)
            .queueSize(Integer.MAX_VALUE)
            .constantTags("instance:foo", "app:bar")
            .originDetectionEnabled(false)
            .build();
        try {
            client.gauge("value", 423);
            server.waitForMessage("my.prefix");

            assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.value:423|g|#app:bar,instance:foo")));
        } finally {
            client.stop();
        }
    }

    @Test(timeout = 5000L)
    public void sends_gauge_constant_tags_only() throws Exception {

        final NonBlockingStatsDClient client = new NonBlockingStatsDClientBuilder()
            .prefix("my.prefix")
            .hostname("localhost")
            .port(STATSD_SERVER_PORT)
            .queueSize(Integer.MAX_VALUE)
            .constantTags("instance:foo", "app:bar")
            .originDetectionEnabled(false)
            .build();
        try {
            client.gauge("value", 423);
            server.waitForMessage("my.prefix");

            assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.value:423|g|#app:bar,instance:foo")));
        } finally {
            client.stop();
        }
    }

    @Test(timeout = 5000L)
    public void sends_gauge_entityID_from_env_deprecated() throws Exception {
        final String entity_value =  "foo-entity";
        environmentVariables.set(NonBlockingStatsDClient.DD_ENTITY_ID_ENV_VAR, entity_value);
        final NonBlockingStatsDClient client = new NonBlockingStatsDClientBuilder()
            .prefix("my.prefix")
            .hostname("localhost")
            .port(STATSD_SERVER_PORT)
            .queueSize(Integer.MAX_VALUE)
            .build();
        try {
            client.gauge("value", 423);
            server.waitForMessage("my.prefix");

            assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.value:423|g|#dd.internal.entity_id:foo-entity")));
        } finally {
            client.stop();
        }
    }

    @Test(timeout = 5000L)
    public void sends_gauge_entityID_from_env() throws Exception {
        final String entity_value =  "foo-entity";
        environmentVariables.set(NonBlockingStatsDClient.DD_ENTITY_ID_ENV_VAR, entity_value);
        final NonBlockingStatsDClient client = new NonBlockingStatsDClientBuilder()
            .prefix("my.prefix")
            .hostname("localhost")
            .port(STATSD_SERVER_PORT)
            .queueSize(Integer.MAX_VALUE)
            .build();
        try {
            client.gauge("value", 423);
            server.waitForMessage();

            assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.value:423|g|#dd.internal.entity_id:foo-entity")));
        } finally {
            client.stop();
        }
    }

    @Test(timeout = 5000L)
    public void sends_gauge_entityID_from_env_and_constant_tags_deprecated() throws Exception {
        final String entity_value =  "foo-entity";
        environmentVariables.set(NonBlockingStatsDClient.DD_ENTITY_ID_ENV_VAR, entity_value);
        final String constantTags = "arbitraryTag:arbitraryValue";
        final NonBlockingStatsDClient client = new NonBlockingStatsDClientBuilder()
            .prefix("my.prefix")
            .hostname("localhost")
            .port(STATSD_SERVER_PORT)
            .queueSize(Integer.MAX_VALUE)
            .constantTags(constantTags)
            .build();
        try {
            client.gauge("value", 423);
            server.waitForMessage("my.prefix");

            assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.value:423|g|#dd.internal.entity_id:foo-entity," + constantTags)));
        } finally {
            client.stop();
        }
    }

    @Test(timeout = 5000L)
    public void sends_gauge_entityID_from_env_and_constant_tags() throws Exception {
        final String entity_value =  "foo-entity";
        environmentVariables.set(NonBlockingStatsDClient.DD_ENTITY_ID_ENV_VAR, entity_value);
        final String constantTags = "arbitraryTag:arbitraryValue";
        final NonBlockingStatsDClient client = new NonBlockingStatsDClientBuilder()
            .prefix("my.prefix")
            .hostname("localhost")
            .port(STATSD_SERVER_PORT)
            .constantTags(constantTags)
            .originDetectionEnabled(false)
            .build();
        try {
            client.gauge("value", 423);
            server.waitForMessage("my.prefix");

            assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.value:423|g|#dd.internal.entity_id:foo-entity," + constantTags)));
        } finally {
            client.stop();
        }
    }

    @Test(timeout = 5000L)
    public void sends_gauge_entityID_from_args_deprecated() throws Exception {
        final String entity_value =  "foo-entity";
        environmentVariables.set(NonBlockingStatsDClient.DD_ENTITY_ID_ENV_VAR, entity_value);

        final NonBlockingStatsDClient client = new NonBlockingStatsDClientBuilder()
            .prefix("my.prefix")
            .hostname("localhost")
            .port(STATSD_SERVER_PORT)
            .queueSize(Integer.MAX_VALUE)
            .constantTags(null)
            .errorHandler(null)
            .entityID(entity_value+"-arg")
            .build();

        try {
            client.gauge("value", 423);
            server.waitForMessage("my.prefix");

            assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.value:423|g|#dd.internal.entity_id:foo-entity-arg")));
        } finally {
            client.stop();
        }
    }

    @Test(timeout = 5000L)
    public void sends_gauge_entityID_from_args() throws Exception {
        final String entity_value =  "foo-entity";
        environmentVariables.set(NonBlockingStatsDClient.DD_ENTITY_ID_ENV_VAR, entity_value);
        final NonBlockingStatsDClient client = new NonBlockingStatsDClientBuilder()
            .prefix("my.prefix")
            .hostname("localhost")
            .port(STATSD_SERVER_PORT)
            .queueSize(Integer.MAX_VALUE)
            .entityID(entity_value+"-arg")
            .build();
        try {
            client.gauge("value", 423);
            server.waitForMessage("my.prefix");

            assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.value:423|g|#dd.internal.entity_id:foo-entity-arg")));
        } finally {
            client.stop();
        }
    }


    @Test(timeout = 5000L)
    public void init_client_from_env_vars() throws Exception {
        final String entity_value =  "foo-entity";
        environmentVariables.set(NonBlockingStatsDClient.DD_DOGSTATSD_PORT_ENV_VAR, Integer.toString(STATSD_SERVER_PORT));
        environmentVariables.set(NonBlockingStatsDClient.DD_AGENT_HOST_ENV_VAR, "localhost");
        final NonBlockingStatsDClient client = new NonBlockingStatsDClientBuilder()
            .prefix("my.prefix")
            .originDetectionEnabled(false)
            .build();
        try {
            client.gauge("value", 423);
            server.waitForMessage("my.prefix");

            assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.value:423|g")));
        } finally {
            client.stop();
        }
    }

    @Test(timeout = 15000L)
    public void checkEnvVars() {
        final Random r = new Random();
        for (final NonBlockingStatsDClient.Literal literal : NonBlockingStatsDClient.Literal.values()) {
            final String envVarName = literal.envName();
            final String randomString = envVarName + "_val_" +r.nextDouble();
            environmentVariables.set(envVarName, randomString);
            final NonBlockingStatsDClient client = new NonBlockingStatsDClientBuilder()
                    .prefix("checkEnvVars")
                    .hostname("localhost")
                    .port(STATSD_SERVER_PORT)
                    .originDetectionEnabled(false)
                    .build();
            server.clear();
            client.gauge("value", 42);
            server.waitForMessage("checkEnvVars.value");
            log.info("passed for '" + literal + "'; env cleaned.");
            assertThat(server.messagesReceived(), hasItem(comparesEqualTo("checkEnvVars.value:42|g|#" +
                    literal.tag() + ":" + randomString)));
            assertThat(server.messagesReceived(), hasItem(comparesEqualTo("checkEnvVars.value:42|g|#" +
                    envVarName.replace("DD_", "").toLowerCase() + ":" + randomString)));
            server.clear();

            environmentVariables.clear(envVarName);
            log.info("passed for '" + literal + "'; env cleaned.");
            client.stop();
        }
    }

    @Test(timeout = 5000L)
    public void sends_gauge_empty_prefix_deprecated() throws Exception {

        final NonBlockingStatsDClient client = new NonBlockingStatsDClientBuilder()
            .prefix("")
            .hostname("localhost")
            .port(STATSD_SERVER_PORT)
            .originDetectionEnabled(false)
            .build();

        try {
            client.gauge("top.level.value", 423);
            server.waitForMessage("top.level");

            assertThat(server.messagesReceived(), hasItem(comparesEqualTo("top.level.value:423|g")));
        } finally {
            client.stop();
        }
    }

    @Test(timeout = 5000L)
    public void sends_gauge_empty_prefix() throws Exception {

        final NonBlockingStatsDClient client = new NonBlockingStatsDClientBuilder()
            .prefix("")
            .hostname("localhost")
            .port(STATSD_SERVER_PORT)
            .originDetectionEnabled(false)
            .build();
        try {
            client.gauge("top.level.value", 423);
            server.waitForMessage("top.level");

            assertThat(server.messagesReceived(), hasItem(comparesEqualTo("top.level.value:423|g")));
        } finally {
            client.stop();
        }
    }

    @Test(timeout = 5000L)
    public void sends_gauge_null_prefix_deprecated() throws Exception {

        final NonBlockingStatsDClient client = new NonBlockingStatsDClientBuilder()
            .prefix(null)
            .hostname("localhost")
            .port(STATSD_SERVER_PORT)
            .originDetectionEnabled(false)
            .build();

        try {
            client.gauge("top.level.value", 423);
            server.waitForMessage("top.level");

            assertThat(server.messagesReceived(), hasItem(comparesEqualTo("top.level.value:423|g")));
        } finally {
            client.stop();
        }
    }

    @Test(timeout = 5000L)
    public void sends_gauge_null_prefix() throws Exception {

        final NonBlockingStatsDClient client = new NonBlockingStatsDClientBuilder()
            .prefix(null)
            .hostname("localhost")
            .port(STATSD_SERVER_PORT)
            .originDetectionEnabled(false)
            .build();
        try {
            client.gauge("top.level.value", 423);
            server.waitForMessage("top.level");

            assertThat(server.messagesReceived(), hasItem(comparesEqualTo("top.level.value:423|g")));
        } finally {
            client.stop();
        }
    }

    @Test(timeout = 5000L)
    public void sends_gauge_no_prefix() throws Exception {

        final NonBlockingStatsDClient no_prefix_client = new NonBlockingStatsDClientBuilder()
            .hostname("localhost")
            .port(STATSD_SERVER_PORT)
            .originDetectionEnabled(false)
            .build();
        try {
            no_prefix_client.gauge("top.level.value", 423);
            server.waitForMessage("top.level");

            assertThat(server.messagesReceived(), hasItem(comparesEqualTo("top.level.value:423|g")));
        } finally {
            no_prefix_client.stop();
        }
    }

    @Test(timeout = 5000L)
    public void sends_event() throws Exception {

        final Event event = Event.builder()
                .withTitle("title1")
                .withText("text1\nline2")
                .withDate(1234567000)
                .withHostname("host1")
                .withPriority(Event.Priority.LOW)
                .withAggregationKey("key1")
                .withAlertType(Event.AlertType.ERROR)
                .withSourceTypeName("sourcetype1")
                .build();
        client.recordEvent(event);
        server.waitForMessage();

        assertThat(
            server.messagesReceived(),
            hasItem(comparesEqualTo("_e{16,12}:my.prefix.title1|text1\\nline2|d:1234567|h:host1|k:key1|p:low|t:error|s:sourcetype1"))
        );
    }

    @Test(timeout = 5000L)
    public void send_unicode_event() throws Exception {
        final Event event = Event.builder()
            .withTitle("Delivery - Daily Settlement Summary Report Delivery — Invoice Cloud succeeded")
            .withText("Delivered — destination.csv").build();
        assertEquals(event.getText(), "Delivered — destination.csv");
        client.recordEvent(event);
        server.waitForMessage();
        assertThat(
            server.messagesReceived(),
            hasItem(comparesEqualTo("_e{89,29}:my.prefix.Delivery - Daily Settlement Summary Report Delivery — Invoice Cloud succeeded|Delivered — destination.csv"))
        );
    }

    @Test(timeout = 5000L)
    public void sends_partial_event() throws Exception {

        final Event event = Event.builder()
                .withTitle("title1")
                .withText("text1")
                .withDate(1234567000)
                .build();
        client.recordEvent(event);
        server.waitForMessage();

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("_e{16,5}:my.prefix.title1|text1|d:1234567")));
    }

    @Test(timeout = 5000L)
    public void sends_event_with_tags() throws Exception {

        final Event event = Event.builder()
                .withTitle("title1")
                .withText("text1")
                .withDate(1234567000)
                .withHostname("host1")
                .withPriority(Event.Priority.LOW)
                .withAggregationKey("key1")
                .withAlertType(Event.AlertType.ERROR)
                .withSourceTypeName("sourcetype1")
                .build();
        client.recordEvent(event, "foo:bar", "baz");
        server.waitForMessage();

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("_e{16,5}:my.prefix.title1|text1|d:1234567|h:host1|k:key1|p:low|t:error|s:sourcetype1|#baz,foo:bar")));
    }

    @Test(timeout = 5000L)
    public void sends_partial_event_with_tags() throws Exception {

        final Event event = Event.builder()
                .withTitle("title1")
                .withText("text1")
                .withDate(1234567000)
                .build();
        client.recordEvent(event, "foo:bar", "baz");
        server.waitForMessage();

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("_e{16,5}:my.prefix.title1|text1|d:1234567|#baz,foo:bar")));
    }

    @Test(timeout = 5000L)
    public void sends_event_empty_prefix_deprecated() throws Exception {

        final NonBlockingStatsDClient client = new NonBlockingStatsDClientBuilder()
            .prefix("")
            .hostname("localhost")
            .port(STATSD_SERVER_PORT)
            .originDetectionEnabled(false)
            .build();
        final Event event = Event.builder()
                .withTitle("title1")
                .withText("text1")
                .withDate(1234567000)
                .withHostname("host1")
                .withPriority(Event.Priority.LOW)
                .withAggregationKey("key1")
                .withAlertType(Event.AlertType.ERROR)
                .withSourceTypeName("sourcetype1")
                .build();
        try {
            client.recordEvent(event, "foo:bar", "baz");
            server.waitForMessage("_e");

            assertThat(server.messagesReceived(), hasItem(comparesEqualTo("_e{6,5}:title1|text1|d:1234567|h:host1|k:key1|p:low|t:error|s:sourcetype1|#baz,foo:bar")));
        } finally {
            client.stop();
        }
    }

    @Test(timeout = 5000L)
    public void sends_event_empty_prefix() throws Exception {

        final NonBlockingStatsDClient client = new NonBlockingStatsDClientBuilder()
            .prefix("")
            .hostname("localhost")
            .port(STATSD_SERVER_PORT)
            .originDetectionEnabled(false)
            .build();

        final Event event = Event.builder()
                .withTitle("title1")
                .withText("text1")
                .withDate(1234567000)
                .withHostname("host1")
                .withPriority(Event.Priority.LOW)
                .withAggregationKey("key1")
                .withAlertType(Event.AlertType.ERROR)
                .withSourceTypeName("sourcetype1")
                .build();
        try {
            client.recordEvent(event, "foo:bar", "baz");
            server.waitForMessage("_e");

            assertThat(server.messagesReceived(), hasItem(comparesEqualTo("_e{6,5}:title1|text1|d:1234567|h:host1|k:key1|p:low|t:error|s:sourcetype1|#baz,foo:bar")));
        } finally {
            client.stop();
        }
    }

    @Test(timeout = 5000L)
    public void sends_no_body_event() throws Exception {
        final Event event = Event.builder()
                .withTitle("title")
                .withDate(1234567000)
                .build();
        client.recordEvent(event);
        server.waitForMessage();

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("_e{15,0}:my.prefix.title||d:1234567")));
    }

    @Test(timeout = 5000L)
    public void sends_service_check() throws Exception {
        final String inputMessage = "\u266c \u2020\u00f8U \n\u2020\u00f8U \u00a5\u00bau|m: T0\u00b5 \u266a"; // "♬ †øU \n†øU ¥ºu|m: T0µ ♪"
        final String outputMessage = "\u266c \u2020\u00f8U \\n\u2020\u00f8U \u00a5\u00bau|m\\: T0\u00b5 \u266a"; // note the escaped colon
        final String[] tags = {"key1:val1", "key2:val2"};
        final ServiceCheck sc = ServiceCheck.builder()
                .withName("my_check.name")
                .withStatus(ServiceCheck.Status.WARNING)
                .withMessage(inputMessage)
                .withHostname("i-abcd1234")
                .withTags(tags)
                .withTimestamp(1420740000)
                .build();

        assertEquals(outputMessage, sc.getEscapedMessage());

        client.serviceCheck(sc);
        server.waitForMessage("_sc");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo(String.format("_sc|my_check.name|1|d:1420740000|h:i-abcd1234|#key2:val2,key1:val1|m:%s",
                outputMessage))));
    }

    @Test(timeout = 5000L)
    public void sends_nan_gauge_to_statsd() throws Exception {
        client.recordGaugeValue("mygauge", Double.NaN);

        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.mygauge:NaN|g")));
    }

    @Test(timeout = 5000L)
    public void sends_set_to_statsd() throws Exception {
        client.recordSetValue("myset", "myuserid");

        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.myset:myuserid|s")));

    }

    @Test(timeout = 5000L)
    public void sends_set_to_statsd_with_tags() throws Exception {
        client.recordSetValue("myset", "myuserid", "foo:bar", "baz");

        server.waitForMessage("my.prefix.myset");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.myset:myuserid|s|#baz,foo:bar")));

    }

    @Test(timeout=5000L)
    public void sends_too_large_message_deprecated() throws Exception {

        final RecordingErrorHandler errorHandler = new RecordingErrorHandler();

        try (final NonBlockingStatsDClient testClient = new NonBlockingStatsDClientBuilder()
            .prefix("")
            .hostname("localhost")
            .port(STATSD_SERVER_PORT)
            .errorHandler(errorHandler)
            .originDetectionEnabled(false)
            .build()) {

            final byte[] messageBytes = new byte[1600];
            final ServiceCheck tooLongServiceCheck = ServiceCheck.builder()
                    .withName("toolong")
                    .withMessage(new String(messageBytes))
                    .withStatus(ServiceCheck.Status.OK)
                    .build();
            testClient.serviceCheck(tooLongServiceCheck);

            final ServiceCheck withinLimitServiceCheck = ServiceCheck.builder()
                    .withName("fine")
                    .withStatus(ServiceCheck.Status.OK)
                    .build();
            testClient.serviceCheck(withinLimitServiceCheck);

            server.waitForMessage("_sc");

            final List<Exception> exceptions = errorHandler.getExceptions();
            assertEquals(1, exceptions.size());
            final Exception exception = exceptions.get(0);
            assertEquals(InvalidMessageException.class, exception.getClass());
            assertTrue(((InvalidMessageException)exception).getInvalidMessage().startsWith("_sc|toolong|"));

            final List<String> messages = server.messagesReceived();
            assertThat(messages, hasItem(comparesEqualTo("_sc|fine|0")));
        }
    }

    @Test(timeout=5000L)
    public void sends_too_large_message() throws Exception {
        final RecordingErrorHandler errorHandler = new RecordingErrorHandler();


        try (final NonBlockingStatsDClient testClient = new NonBlockingStatsDClientBuilder()
                .prefix("my.prefix")
                .hostname("localhost")
                .port(STATSD_SERVER_PORT)
                .errorHandler(errorHandler)
                .originDetectionEnabled(false)
                .build()) {

            final byte[] messageBytes = new byte[1600];
            final ServiceCheck tooLongServiceCheck = ServiceCheck.builder()
                    .withName("toolong")
                    .withMessage(new String(messageBytes))
                    .withStatus(ServiceCheck.Status.OK)
                    .build();
            testClient.serviceCheck(tooLongServiceCheck);

            final ServiceCheck withinLimitServiceCheck = ServiceCheck.builder()
                    .withName("fine")
                    .withStatus(ServiceCheck.Status.OK)
                    .build();
            testClient.serviceCheck(withinLimitServiceCheck);

            server.waitForMessage("_sc");

            final List<Exception> exceptions = errorHandler.getExceptions();
            assertEquals(1, exceptions.size());
            final Exception exception = exceptions.get(0);
            assertEquals(InvalidMessageException.class, exception.getClass());
            assertTrue(((InvalidMessageException)exception).getInvalidMessage().startsWith("_sc|toolong|"));
            // assertEquals(BufferOverflowException.class, exception.getClass());

            final List<String> messages = server.messagesReceived();
            assertThat(messages, hasItem(comparesEqualTo("_sc|fine|0")));
        }
    }

    @Test(timeout=5000L)
    public void sends_telemetry_elsewhere() throws Exception {
        final RecordingErrorHandler errorHandler = new RecordingErrorHandler();
        final DummyStatsDServer telemetryServer = new UDPDummyStatsDServer(STATSD_SERVER_PORT+10);
        final NonBlockingStatsDClient testClient = new NonBlockingStatsDClientBuilder()
            .prefix("my.prefix")
            .hostname("localhost")
            .port(STATSD_SERVER_PORT)
            .telemetryHostname("localhost")
            .telemetryPort(STATSD_SERVER_PORT+10)
            .telemetryFlushInterval(3000)
            .errorHandler(errorHandler)
            .originDetectionEnabled(false)
            .build();

        try {
            testClient.gauge("top.level.value", 423);
            server.waitForMessage("my.prefix");

            assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.top.level.value:423|g")));

            telemetryServer.waitForMessage();

            // 8 messages in telemetry batch
            final List<String> messages = telemetryServer.messagesReceived();
            assertEquals(17, messages.size());
            assertThat(messages, hasItem(startsWith("datadog.dogstatsd.client.metrics:1|c")));
            assertThat(messages, hasItem(startsWith("datadog.dogstatsd.client.events:0|c")));
            assertThat(messages, hasItem(startsWith("datadog.dogstatsd.client.service_checks:0|c")));
            assertThat(messages, hasItem(startsWith("datadog.dogstatsd.client.bytes_sent:32|c")));
            assertThat(messages, hasItem(startsWith("datadog.dogstatsd.client.bytes_dropped:0|c")));
            assertThat(messages, hasItem(startsWith("datadog.dogstatsd.client.packets_sent:1|c")));
            assertThat(messages, hasItem(startsWith("datadog.dogstatsd.client.packets_dropped:0|c")));
            assertThat(messages, hasItem(startsWith("datadog.dogstatsd.client.packets_dropped_queue:0|c")));
            assertThat(messages, hasItem(startsWith("datadog.dogstatsd.client.aggregated_context:0|c")));
        } finally {
            testClient.stop();
            telemetryServer.close();
        }
    }

    @Test(timeout=5000L)
    public void testBasicGaugeAggregation() throws Exception {
        final RecordingErrorHandler errorHandler = new RecordingErrorHandler();
        final NonBlockingStatsDClient testClient = new NonBlockingStatsDClientBuilder()
            .prefix("my.prefix")
            .hostname("localhost")
            .port(STATSD_SERVER_PORT)
            .enableTelemetry(false)  // don't want additional packets
            .enableAggregation(true)
            .aggregationFlushInterval(3000)
            .errorHandler(errorHandler)
            .originDetectionEnabled(false)
            .build();

        try {
            for (int i=0 ; i<10 ; i++) {
                testClient.gauge("top.level.value", i);
            }
            server.waitForMessage("my.prefix");

            List<String> messages = server.messagesReceived();

            assertThat(messages.size(), comparesEqualTo(1));
            assertThat(messages, hasItem(comparesEqualTo("my.prefix.top.level.value:9|g")));

        } finally {
            testClient.stop();
        }
    }

    @Test(timeout=5000L)
    public void testBasicCountAggregation() throws Exception {
        final RecordingErrorHandler errorHandler = new RecordingErrorHandler();
        final NonBlockingStatsDClient testClient = new NonBlockingStatsDClientBuilder()
            .prefix("my.prefix")
            .hostname("localhost")
            .port(STATSD_SERVER_PORT)
            .enableTelemetry(false)  // don't want additional packets
            .enableAggregation(true)
            .aggregationFlushInterval(3000)
            .errorHandler(errorHandler)
            .originDetectionEnabled(false)
            .build();

        try {
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

        } finally {
            testClient.stop();
        }
    }

    @Test(timeout=5000L)
    public void testSampledCountAggregation() throws Exception {
        final RecordingErrorHandler errorHandler = new RecordingErrorHandler();
        final NonBlockingStatsDClient testClient = new NonBlockingStatsDClientBuilder()
            .prefix("my.prefix")
            .hostname("localhost")
            .port(STATSD_SERVER_PORT)
            .enableTelemetry(false)  // don't want additional packets
            .enableAggregation(true)
            .aggregationFlushInterval(3000)
            .errorHandler(errorHandler)
            .originDetectionEnabled(false)
            .build();

        try {
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

        } finally {
            testClient.stop();
        }
    }

    @Test(timeout=5000L)
    public void testBasicSetAggregation() throws Exception {
        final RecordingErrorHandler errorHandler = new RecordingErrorHandler();
        final NonBlockingStatsDClient testClient = new NonBlockingStatsDClientBuilder()
            .prefix("my.prefix")
            .hostname("localhost")
            .port(STATSD_SERVER_PORT)
            .enableTelemetry(false)  // don't want additional packets
            .enableAggregation(true)
            .aggregationFlushInterval(3000)
            .errorHandler(errorHandler)
            .originDetectionEnabled(false)
            .build();

        try {
            for (int i=0 ; i<10 ; i++) {
                testClient.recordSetValue("top.level.set", "foo", null);
                testClient.recordSetValue("top.level.set", "bar", null);
            }

            server.waitForMessage("my.prefix");

            List<String> messages = server.messagesReceived();

            assertThat(messages.size(), comparesEqualTo(2));
            assertThat(messages, hasItem(comparesEqualTo("my.prefix.top.level.set:foo|s")));
            assertThat(messages, hasItem(comparesEqualTo("my.prefix.top.level.set:bar|s")));

        } finally {
            testClient.stop();
        }
    }

    @Test(timeout=5000L)
    public void testAggregationTelemetry() throws Exception {
        final RecordingErrorHandler errorHandler = new RecordingErrorHandler();
        final NonBlockingStatsDClient testClient = new NonBlockingStatsDClientBuilder()
            .hostname("localhost")
            .port(STATSD_SERVER_PORT)
            .enableAggregation(true)
            .aggregationFlushInterval(3000)
            .telemetryFlushInterval(3000)
            .errorHandler(errorHandler)
            .originDetectionEnabled(false)
            .build();

        try {
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

        } finally {
            testClient.stop();
        }
    }

    @Test(timeout=5000L)
    public void testBasicUnaggregatedMetrics() throws Exception {
        final RecordingErrorHandler errorHandler = new RecordingErrorHandler();
        final NonBlockingStatsDClient testClient = new NonBlockingStatsDClientBuilder()
            .prefix("my.prefix")
            .hostname("localhost")
            .port(STATSD_SERVER_PORT)
            .enableTelemetry(false)  // don't want additional packets
            .enableAggregation(true)
            .aggregationFlushInterval(3000)
            .errorHandler(errorHandler)
            .originDetectionEnabled(false)
            .build();

        try {
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

        } finally {
            testClient.stop();
        }
    }

    @Test
    public void testMessageHashcode() throws Exception {

        StatsDTestMessage previous = new StatsDTestMessage<Long>("my.count", Message.Type.COUNT, Long.valueOf(1), 0, new String[0]) {
            @Override protected void writeValue(StringBuilder builder) {
                builder.append(this.value);
            };
        };
        StatsDTestMessage previousNewAspectString = new StatsDTestMessage<Long>(new String("my.count"), Message.Type.COUNT, Long.valueOf(1), 0, new String[0]) {
            @Override protected void writeValue(StringBuilder builder) {
                builder.append(this.value);
            };
        };
        StatsDTestMessage previousTagged =
            new StatsDTestMessage<Long>("my.count", Message.Type.COUNT, Long.valueOf(1), 0, new String[] {"foo", "bar"}) {

            @Override protected void writeValue(StringBuilder builder) {
                builder.append(this.value);
            };
        };

        StatsDTestMessage next = new StatsDTestMessage<Long>("my.count", Message.Type.COUNT, Long.valueOf(1), 0, new String[0]) {
            @Override protected void writeValue(StringBuilder builder) {
                builder.append(this.value);
            };
        };
        StatsDTestMessage nextTagged =
            new StatsDTestMessage<Long>("my.count", Message.Type.COUNT, Long.valueOf(1), 0, new String[] {"foo", "bar"}) {

            @Override protected void writeValue(StringBuilder builder) {
                builder.append(this.value);
            };
        };

        assertEquals(previous.hashCode(), next.hashCode());
        assertEquals(previousTagged.hashCode(), nextTagged.hashCode());
        assertEquals(previous.hashCode(), previousNewAspectString.hashCode());
        assertEquals(previous, previousNewAspectString);

        class TestAlphaNumericMessage extends AlphaNumericMessage {
            public TestAlphaNumericMessage(String aspect, Type type, String value, String[] tags) {
                super(aspect, type, value, tags);
            }

            @Override
            boolean writeTo(StringBuilder builder, int capacity, String containerID) {
                return false;
            }
        }
        AlphaNumericMessage alphaNum1 = new TestAlphaNumericMessage("my.count", Message.Type.COUNT, "value", new String[] {"tag"});
        AlphaNumericMessage alphaNum2 = new TestAlphaNumericMessage(new String("my.count"), Message.Type.COUNT, new String("value"), new String[]{new String("tag")});
        assertEquals(alphaNum1, alphaNum2);

    }

    @Test(timeout = 5000L)
    public void shutdown_test() throws Exception {
        final int port = 17256;
        final int qSize = 256;
        final DummyStatsDServer server = new UDPDummyStatsDServer(port);

        final NonBlockingStatsDClientBuilder builder = new SlowStatsDNonBlockingStatsDClientBuilder().prefix("")
            .hostname("localhost")
            .originDetectionEnabled(false)
            .port(port);
        final SlowStatsDNonBlockingStatsDClient client = ((SlowStatsDNonBlockingStatsDClientBuilder)builder).build();

        try {
            client.count("mycounter", 5);
            assertEquals(0, server.messagesReceived().size());
            server.waitForMessage();
            assertEquals(1, server.messagesReceived().size());
        } finally {
            client.stop();
            server.close();
            assertEquals(0, client.getLock().getCount());
        }
    }

    @Test(timeout = 5000L)
    public void sends_gauge_with_containerID() throws Exception {
        clientWithContainerID.gauge("value", 423, "foo");
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.value:423|g|#foo|c:fake-container-id")));
    }

    @Test(timeout = 5000L)
    public void sends_counter_with_containerID() throws Exception {
        clientWithContainerID.count("value", 423, "foo");
        server.waitForMessage("my.prefix");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.value:423|c|#foo|c:fake-container-id")));
    }

    @Test(timeout = 5000L)
    public void sends_set_with_containerID() throws Exception {
        clientWithContainerID.recordSetValue("myset", "myuserid", "foo");
        server.waitForMessage("my.prefix.myset");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.myset:myuserid|s|#foo|c:fake-container-id")));
    }

    @Test(timeout = 5000L)
    public void sends_service_check_with_containerID() throws Exception {
        final String inputMessage = "\u266c \u2020\u00f8U \n\u2020\u00f8U \u00a5\u00bau|m: T0\u00b5 \u266a"; // "♬ †øU \n†øU ¥ºu|m: T0µ ♪"
        final String outputMessage = "\u266c \u2020\u00f8U \\n\u2020\u00f8U \u00a5\u00bau|m\\: T0\u00b5 \u266a"; // note the escaped colon
        final String[] tags = {"key1:val1", "key2:val2"};
        final ServiceCheck sc = ServiceCheck.builder()
                .withName("my_check.name")
                .withStatus(ServiceCheck.Status.WARNING)
                .withMessage(inputMessage)
                .withHostname("i-abcd1234")
                .withTags(tags)
                .withTimestamp(1420740000)
                .build();

        assertEquals(outputMessage, sc.getEscapedMessage());

        clientWithContainerID.serviceCheck(sc);
        server.waitForMessage("_sc");

        assertThat(server.messagesReceived(), hasItem(comparesEqualTo(String.format("_sc|my_check.name|1|d:1420740000|h:i-abcd1234|#key2:val2,key1:val1|m:%s|c:fake-container-id",
                outputMessage))));
    }

    @Test(timeout = 5000L)
    public void sends_event_with_containerID() throws Exception {

        final Event event = Event.builder()
                .withTitle("title1")
                .withText("text1\nline2")
                .withDate(1234567000)
                .withHostname("host1")
                .withPriority(Event.Priority.LOW)
                .withAggregationKey("key1")
                .withAlertType(Event.AlertType.ERROR)
                .withSourceTypeName("sourcetype1")
                .build();

        clientWithContainerID.recordEvent(event);
        server.waitForMessage();

        assertThat(
            server.messagesReceived(),
            hasItem(comparesEqualTo("_e{16,12}:my.prefix.title1|text1\\nline2|d:1234567|h:host1|k:key1|p:low|t:error|s:sourcetype1|c:fake-container-id"))
        );
    }

    @Test(timeout = 5000L)
    public void test_entity_id_and_container_id() throws Exception {
        final String entity_value =  "foo-entity";
        environmentVariables.set(NonBlockingStatsDClient.DD_ENTITY_ID_ENV_VAR, entity_value);
        final NonBlockingStatsDClient client = new NonBlockingStatsDClientBuilder()
        .prefix("my.prefix")
        .hostname("localhost")
        .port(STATSD_SERVER_PORT)
        .enableTelemetry(false)
        .containerID("fake-container-id")
        .build();
        try {
            client.gauge("value", 423);
            server.waitForMessage();

            assertThat(server.messagesReceived(), hasItem(comparesEqualTo("my.prefix.value:423|g|#dd.internal.entity_id:foo-entity|c:fake-container-id")));
        } finally {
            client.stop();
        }
    }

    @Test(timeout = 5000L)
    public void origin_detection_env_false() throws Exception {
        environmentVariables.set(NonBlockingStatsDClient.ORIGIN_DETECTION_ENABLED_ENV_VAR, "false");

        final NonBlockingStatsDClient client = new NonBlockingStatsDClientBuilder()
            .prefix("my.prefix")
            .hostname("localhost")
            .port(STATSD_SERVER_PORT)
            .queueSize(Integer.MAX_VALUE)
            .errorHandler(null)
            .enableTelemetry(false)
            .build();

        assertFalse(client.isOriginDetectionEnabled(null, NonBlockingStatsDClient.DEFAULT_ENABLE_ORIGIN_DETECTION));
        environmentVariables.clear(NonBlockingStatsDClient.ORIGIN_DETECTION_ENABLED_ENV_VAR);
    }

    @Test(timeout = 5000L)
    public void origin_detection_env_unknown() throws Exception {
        environmentVariables.set(NonBlockingStatsDClient.ORIGIN_DETECTION_ENABLED_ENV_VAR, "unknown"); // default to true

        final NonBlockingStatsDClient client = new NonBlockingStatsDClientBuilder()
            .prefix("my.prefix")
            .hostname("localhost")
            .port(STATSD_SERVER_PORT)
            .queueSize(Integer.MAX_VALUE)
            .errorHandler(null)
            .enableTelemetry(false)
            .build();

        assertTrue(client.isOriginDetectionEnabled(null, NonBlockingStatsDClient.DEFAULT_ENABLE_ORIGIN_DETECTION));
        environmentVariables.clear(NonBlockingStatsDClient.ORIGIN_DETECTION_ENABLED_ENV_VAR);
    }

    @Test(timeout = 5000L)
    public void origin_detection_env_unset() throws Exception {
        final NonBlockingStatsDClient client = new NonBlockingStatsDClientBuilder()
            .prefix("my.prefix")
            .hostname("localhost")
            .port(STATSD_SERVER_PORT)
            .queueSize(Integer.MAX_VALUE)
            .errorHandler(null)
            .enableTelemetry(false)
            .build();

        assertTrue(client.isOriginDetectionEnabled(null, NonBlockingStatsDClient.DEFAULT_ENABLE_ORIGIN_DETECTION));
    }

    @Test(timeout = 5000L)
    public void origin_detection_arg_false() throws Exception {
        final NonBlockingStatsDClient client = new NonBlockingStatsDClientBuilder()
            .prefix("my.prefix")
            .hostname("localhost")
            .port(STATSD_SERVER_PORT)
            .queueSize(Integer.MAX_VALUE)
            .errorHandler(null)
            .enableTelemetry(false)
            .build();

        assertFalse(client.isOriginDetectionEnabled(null, false));
    }

    private static class SlowStatsDNonBlockingStatsDClient extends NonBlockingStatsDClient {

        private CountDownLatch lock;

        SlowStatsDNonBlockingStatsDClient(NonBlockingStatsDClientBuilder builder) throws StatsDClientException {
            super(builder);

            lock = new CountDownLatch(1);
        }

        public CountDownLatch getLock() {
            return this.lock;
        }

        @Override
        public void stop() {
            super.stop();
            lock.countDown();
        }

    };

    private static class SlowStatsDNonBlockingStatsDClientBuilder extends NonBlockingStatsDClientBuilder {

        @Override
        public SlowStatsDNonBlockingStatsDClient build() throws StatsDClientException {
            return new SlowStatsDNonBlockingStatsDClient(resolve());
        }
    }

    private static class NonsamplingClient extends NonBlockingStatsDClient {
	NonsamplingClient(NonBlockingStatsDClientBuilder builder) {
	    super(builder);
	}
	@Override
	protected boolean isInvalidSample(double sampleRate) {
	    return false;
	}
    }

    private static class NonsamplingClientBuilder extends NonBlockingStatsDClientBuilder {
	@Override
	public NonsamplingClient build() throws StatsDClientException {
	    return new NonsamplingClient(resolve());
	}
    }

    @Test(timeout = 5000L)
    public void nonsampling_client_test() throws Exception {
        final int port = 17256;
        final DummyStatsDServer server = new UDPDummyStatsDServer(port);

	final NonBlockingStatsDClientBuilder builder = new NonsamplingClientBuilder()
	    .prefix("")
            .hostname("localhost")
        .originDetectionEnabled(false)
	    .port(port);

	final NonsamplingClient client = ((NonsamplingClientBuilder)builder).build();

        try {
            client.gauge("test.gauge", 42.0, 0.0);
	    server.waitForMessage();
	    List<String> messages = server.messagesReceived();
            assertEquals(1, messages.size());
            assertEquals(messages.get(0), "test.gauge:42|g|@0.000000");
        } finally {
            client.stop();
            server.close();
        }
    }

    // Test that in the blocking mode metrics are written out to the socket before close() returns.
    volatile boolean blocking_close_sent = false;
    @Test(timeout = 5000L)
    public void blocking_close_test() throws Exception {
        final int port = 17256;
        final byte[] expect = "test:1|g\n".getBytes(StandardCharsets.UTF_8);
        NonBlockingStatsDClientBuilder builder = new NonBlockingStatsDClientBuilder() {
                @Override
                public NonBlockingStatsDClient build() {
                    this.originDetectionEnabled(false);
                    return new NonBlockingStatsDClient(resolve()) {
                        @Override
                        ClientChannel createByteChannel(Callable<SocketAddress> addressLookup, int timeout, int connectionTimeout, int bufferSize) throws Exception {
                            return new DatagramClientChannel(addressLookup.call()) {
                                @Override
                                public int write(ByteBuffer data) throws IOException {
                                    // The test should pass without the sleep, but if there is a
                                    // race with the assert below, make it worse.
                                    try {
                                        Thread.sleep(100);
                                    } catch (InterruptedException e) {
                                        return 0;
                                    }
                                    boolean sent = data.equals(ByteBuffer.wrap(expect));
                                    int n = super.write(data);
                                    blocking_close_sent = sent;
                                    return n;
                                }
                            };
                        }
                    };
                }
            };
        NonBlockingStatsDClient client = builder.hostname("localhost").port(port).blocking(true).build();
        client.gauge("test", 1);
        client.close();
        assertEquals(true, blocking_close_sent);
    }


    @Test(timeout = 5000L)
    public void failed_write_buffer() throws Exception {
        final BlockingQueue sync = new ArrayBlockingQueue(1);
        final IOException marker = new IOException();
        NonBlockingStatsDClientBuilder builder = new NonBlockingStatsDClientBuilder() {
                @Override
                public NonBlockingStatsDClient build() {
                    this.originDetectionEnabled(false);
                    this.bufferPoolSize(1);
                    return new NonBlockingStatsDClient(resolve()) {
                        @Override
                        ClientChannel createByteChannel(Callable<SocketAddress> addressLookup, int timeout, int connectionTimeout, int bufferSize) throws Exception {
                            return new DatagramClientChannel(addressLookup.call()) {
                                @Override
                                public int write(ByteBuffer data) throws IOException {
                                    try {
                                        sync.put(new Object());
                                    } catch (InterruptedException e) {
                                    }
                                    throw marker;
                                }
                            };
                        }
                    };
                }
            };

        final BlockingQueue errors = new LinkedBlockingQueue();
        NonBlockingStatsDClient client = builder
            .hostname("localhost")
            .blocking(true)
            .errorHandler(new StatsDClientErrorHandler() {
                    @Override
                    public void handle(Exception ex) {
                        if (ex == marker) {
                            return;
                        }
                        System.out.println(ex.toString());
                        try {
                            errors.put(ex);
                        } catch (InterruptedException e) {
                        }
                    }

                })
            .build();

        client.gauge("test", 1);
        sync.take();
        client.gauge("test", 1);
        client.stop();

        assertEquals(0, errors.size());
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
}
