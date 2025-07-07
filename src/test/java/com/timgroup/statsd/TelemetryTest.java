package com.timgroup.statsd;

import java.util.logging.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.equalTo;

@RunWith(Parameterized.class)
public class TelemetryTest {
    private static final StatsDClientErrorHandler NO_OP_HANDLER = new StatsDClientErrorHandler() {
        @Override public void handle(final Exception ex) { /* No-op */ }
    };

    private static final StatsDClientErrorHandler LOGGING_HANDLER = new StatsDClientErrorHandler() {

        Logger log = Logger.getLogger(StatsDClientErrorHandler.class.getName());
        @Override public void handle(final Exception ex) {
            log.warning("Got exception: " + ex);
        }
    };

    // fakeProcessor store messages from the telemetry only
    public static class FakeProcessor extends StatsDProcessor {
        public final List<Message> messages = new ArrayList<>();

        FakeProcessor(final StatsDClientErrorHandler handler) throws Exception {
            super(0, handler, 0, 1, 1, 0, 0, new StatsDThreadFactory());
        }


        private class FakeProcessingTask extends StatsDProcessor.ProcessingTask {
            @Override
            protected void processLoop() {}

            @Override Message getMessage() { return null; }

            @Override boolean haveMessages() { return false; }
        }

        @Override
        public synchronized boolean send(final Message msg) {
            messages.add(msg);
            return true;
        }

        void startWorkers() {}

        @Override
        protected ProcessingTask createProcessingTask() {
            return new FakeProcessingTask();
        }

        public List<Message> getMessages() {
            return messages;
        }

        protected synchronized List<String> getMessagesAsStrings() {
            StringBuilder sb = new StringBuilder();
            ArrayList<String> stringMessages = new ArrayList<>(messages.size());
            for(Message m : messages) {
                sb.setLength(0);
                m.writeTo(sb, Integer.MAX_VALUE);
                stringMessages.add(sb.toString());
            }
            return stringMessages;
        }

        public void clear() {
            try {
            messages.clear();
            } catch (Exception e) {}
        }
    }

    private static NonBlockingStatsDClient client;

    private static UDPDummyStatsDServer server;
    private static FakeProcessor fakeProcessor;

    private static String computeTelemetryTags() throws IOException, Exception {
        Properties properties = new Properties();
        properties.load(TelemetryTest.class.getClassLoader().getResourceAsStream(
            "dogstatsd/version.properties"));
        return "client:java," + NonBlockingStatsDClient.CLIENT_VERSION_TAG + properties.getProperty("dogstatsd_client_version") + ",client_transport:udp";
    }

    private static String telemetryTags;
    private final String containerID;
    private final String tagsCardinality;
    private final String tail;

    @Parameters
    public static Object[][] parameters() {
        return TestHelpers.permutations(new Object[][]{
            { null, "my-fake-container-id" },
            { null, "none" },
        });
    }

    @Rule
    public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

    public TelemetryTest(String containerID, String tagsCardinality) {
        this.containerID = containerID;
        this.tagsCardinality = tagsCardinality;

        StringBuilder sb = new StringBuilder();
        if (tagsCardinality != null) {
            sb.append("|card:").append(tagsCardinality);
        }
        if (containerID != null) {
            sb.append("|c:").append(containerID);
        }
        sb.append("\n");
        tail = sb.toString();
    }

    @Before
    public void start() throws IOException, Exception {
        server = new UDPDummyStatsDServer(0);
        fakeProcessor = new FakeProcessor(LOGGING_HANDLER);

        environmentVariables.set("DD_CARDINALITY", tagsCardinality);

        NonBlockingStatsDClientBuilder builder = new NonBlockingStatsDClientBuilder()
            .prefix("my.prefix")
            .hostname("localhost")
            .constantTags("test")
            .port(server.getPort())
            .originDetectionEnabled(false)
            .enableTelemetry(false); // disable telemetry so we can control calls to "flush"
        if (containerID != null) {
            builder.containerID(containerID);
        }
        client = builder.build();
        client.telemetryStatsDProcessor = fakeProcessor;
        telemetryTags = computeTelemetryTags();
    }

    @After
    public void stop() throws Exception {
        try {
            client.stop();
            server.close();
        } catch (java.io.IOException e) {
            return;
        }
    }

    @Test(timeout = 5000L)
    public void telemetry_incrManuallyIncrData() throws Exception {

        client.telemetry.incrMetricsSent(1);
        client.telemetry.incrGaugeSent(1);
        client.telemetry.incrCountSent(1);
        client.telemetry.incrSetSent(1);
        client.telemetry.incrHistogramSent(1);
        client.telemetry.incrDistributionSent(1);
        client.telemetry.incrMetricsSent(1, Message.Type.GAUGE);  // adds to metricsSent
        client.telemetry.incrMetricsSent(1, Message.Type.COUNT);  // adds to metricsSent
        client.telemetry.incrMetricsSent(1, Message.Type.SET);  // adds to metricsSent
        client.telemetry.incrMetricsSent(1, Message.Type.HISTOGRAM);  // adds to metricsSent
        client.telemetry.incrMetricsSent(1, Message.Type.DISTRIBUTION);  // adds to metricsSent
        client.telemetry.incrEventsSent(2);
        client.telemetry.incrServiceChecksSent(3);
        client.telemetry.incrBytesSent(4);
        client.telemetry.incrBytesDropped(5);
        client.telemetry.incrPacketSent(6);
        client.telemetry.incrPacketDropped(7);
        client.telemetry.incrPacketDroppedQueue(8);
        client.telemetry.incrAggregatedContexts(9);
        client.telemetry.incrAggregatedGaugeContexts(10);
        client.telemetry.incrAggregatedCountContexts(11);
        client.telemetry.incrAggregatedSetContexts(12);

        assertThat(client.telemetry.metricsSent.get(), equalTo(6));
        assertThat(client.telemetry.gaugeSent.get(), equalTo(2));
        assertThat(client.telemetry.countSent.get(), equalTo(2));
        assertThat(client.telemetry.setSent.get(), equalTo(2));
        assertThat(client.telemetry.histogramSent.get(), equalTo(2));
        assertThat(client.telemetry.distributionSent.get(), equalTo(2));
        assertThat(client.telemetry.eventsSent.get(), equalTo(2));
        assertThat(client.telemetry.serviceChecksSent.get(), equalTo(3));
        assertThat(client.telemetry.bytesSent.get(), equalTo(4));
        assertThat(client.telemetry.bytesDropped.get(), equalTo(5));
        assertThat(client.telemetry.packetsSent.get(), equalTo(6));
        assertThat(client.telemetry.packetsDropped.get(), equalTo(7));
        assertThat(client.telemetry.packetsDroppedQueue.get(), equalTo(8));
        assertThat(client.telemetry.aggregatedContexts.get(), equalTo(9));
        assertThat(client.telemetry.aggregatedGaugeContexts.get(), equalTo(10));
        assertThat(client.telemetry.aggregatedCountContexts.get(), equalTo(11));
        assertThat(client.telemetry.aggregatedSetContexts.get(), equalTo(12));

        client.telemetry.flush();

        assertThat(client.telemetry.metricsSent.get(), equalTo(0));
        assertThat(client.telemetry.gaugeSent.get(), equalTo(0));
        assertThat(client.telemetry.countSent.get(), equalTo(0));
        assertThat(client.telemetry.setSent.get(), equalTo(0));
        assertThat(client.telemetry.histogramSent.get(), equalTo(0));
        assertThat(client.telemetry.distributionSent.get(), equalTo(0));
        assertThat(client.telemetry.eventsSent.get(), equalTo(0));
        assertThat(client.telemetry.serviceChecksSent.get(), equalTo(0));
        assertThat(client.telemetry.bytesSent.get(), equalTo(0));
        assertThat(client.telemetry.bytesDropped.get(), equalTo(0));
        assertThat(client.telemetry.packetsSent.get(), equalTo(0));
        assertThat(client.telemetry.packetsDropped.get(), equalTo(0));
        assertThat(client.telemetry.packetsDroppedQueue.get(), equalTo(0));
        assertThat(client.telemetry.aggregatedContexts.get(), equalTo(0));
        assertThat(client.telemetry.aggregatedGaugeContexts.get(), equalTo(0));
        assertThat(client.telemetry.aggregatedCountContexts.get(), equalTo(0));
        assertThat(client.telemetry.aggregatedSetContexts.get(), equalTo(0));

        List<String> statsdMessages = fakeProcessor.getMessagesAsStrings() ;

        assertThat(statsdMessages,
                hasItem("datadog.dogstatsd.client.metrics:6|c|#test," + telemetryTags + tail));

        assertThat(statsdMessages,
                hasItem("datadog.dogstatsd.client.metrics_by_type:2|c|#test," + telemetryTags + ",metrics_type:gauge" + tail));

        assertThat(statsdMessages,
                hasItem("datadog.dogstatsd.client.metrics_by_type:2|c|#test," + telemetryTags + ",metrics_type:count" + tail));

        assertThat(statsdMessages,
                hasItem("datadog.dogstatsd.client.metrics_by_type:2|c|#test," + telemetryTags + ",metrics_type:set" + tail));

        assertThat(statsdMessages,
                hasItem("datadog.dogstatsd.client.metrics_by_type:2|c|#test," + telemetryTags + ",metrics_type:histogram" + tail));

        assertThat(statsdMessages,
                hasItem("datadog.dogstatsd.client.metrics_by_type:2|c|#test," + telemetryTags + ",metrics_type:distribution" + tail));

        assertThat(statsdMessages,
                hasItem("datadog.dogstatsd.client.events:2|c|#test," + telemetryTags + tail));

        assertThat(statsdMessages,
                hasItem("datadog.dogstatsd.client.service_checks:3|c|#test," + telemetryTags + tail));

        assertThat(statsdMessages,
                hasItem("datadog.dogstatsd.client.bytes_sent:4|c|#test," + telemetryTags + tail));

        assertThat(statsdMessages,
                hasItem("datadog.dogstatsd.client.bytes_dropped:5|c|#test," + telemetryTags + tail));

        assertThat(statsdMessages,
                hasItem("datadog.dogstatsd.client.packets_sent:6|c|#test," + telemetryTags + tail));

        assertThat(statsdMessages,
                hasItem("datadog.dogstatsd.client.packets_dropped:7|c|#test," + telemetryTags + tail));

        assertThat(statsdMessages,
                hasItem("datadog.dogstatsd.client.packets_dropped_queue:8|c|#test," + telemetryTags + tail));

        assertThat(statsdMessages,
                hasItem("datadog.dogstatsd.client.aggregated_context:9|c|#test," + telemetryTags + tail));

        assertThat(statsdMessages,
                hasItem("datadog.dogstatsd.client.aggregated_context_by_type:10|c|#test," + telemetryTags + ",metrics_type:gauge" + tail));

        assertThat(statsdMessages,
               hasItem("datadog.dogstatsd.client.aggregated_context_by_type:11|c|#test," + telemetryTags + ",metrics_type:count" + tail));

        assertThat(statsdMessages,
                hasItem("datadog.dogstatsd.client.aggregated_context_by_type:12|c|#test," + telemetryTags + ",metrics_type:set" + tail));
    }

    @Test(timeout = 5000L)
    public void telemetry_incrMetricsSent() throws Exception {
        client.count("mycount", 24);

        // wait for the "mycount" to be sent
        server.waitForMessage();
        server.clear();
        fakeProcessor.clear();

        assertThat(client.telemetry.metricsSent.get(), equalTo(1));
        client.telemetry.flush();
        assertThat(client.telemetry.metricsSent.get(), equalTo(0));

        List<String> statsdMessages = fakeProcessor.getMessagesAsStrings() ;

        assertThat(statsdMessages,
                   hasItem("datadog.dogstatsd.client.metrics:1|c|#test," + telemetryTags + tail));

        assertThat(statsdMessages,
                   hasItem("datadog.dogstatsd.client.events:0|c|#test," + telemetryTags + tail));

        assertThat(statsdMessages,
                   hasItem("datadog.dogstatsd.client.service_checks:0|c|#test," + telemetryTags + tail));

        assertThat(statsdMessages,
                   hasItem(String.format("datadog.dogstatsd.client.bytes_sent:%d|c|#test,%s%s", 28 + tail.length(), telemetryTags, tail)));

        assertThat(statsdMessages,
                   hasItem("datadog.dogstatsd.client.bytes_dropped:0|c|#test," + telemetryTags + tail));

        assertThat(statsdMessages,
                   hasItem("datadog.dogstatsd.client.packets_sent:1|c|#test," + telemetryTags + tail));

        assertThat(statsdMessages,
                   hasItem("datadog.dogstatsd.client.packets_dropped:0|c|#test," + telemetryTags + tail));

        assertThat(statsdMessages,
                   hasItem("datadog.dogstatsd.client.packets_dropped_queue:0|c|#test," + telemetryTags + tail));

        assertThat(statsdMessages,
                   hasItem("datadog.dogstatsd.client.aggregated_context:0|c|#test," + telemetryTags + tail));
    }

    @Test(timeout = 5000L)
    public void telemetry_incrDataSent() throws Exception {
        client.gauge("gauge", 24);

        final ServiceCheck sc = ServiceCheck.builder()
                .withName("my_check.name")
                .withStatus(ServiceCheck.Status.WARNING)
                .withMessage("test message")
                .withHostname("i-abcd1234")
                .withTimestamp(1420740000)
                .build();
        client.serviceCheck(sc);

        final Event event = Event.builder()
                .withTitle("title1")
                .withText("text1\nline2")
                .withDate(1234567000)
                .withHostname("host1")
                .withPriority(Event.Priority.LOW)
                .withAggregationKey("key1")
                .withAlertType(Event.AlertType.ERROR)
                .build();
        client.recordEvent(event);

        server.waitForMessage();

        assertThat(client.telemetry.metricsSent.get(), equalTo(1));
        assertThat(client.telemetry.eventsSent.get(), equalTo(1));
        assertThat(client.telemetry.serviceChecksSent.get(), equalTo(1));

        client.telemetry.flush();

        assertThat(client.telemetry.metricsSent.get(), equalTo(0));
        assertThat(client.telemetry.eventsSent.get(), equalTo(0));
        assertThat(client.telemetry.serviceChecksSent.get(), equalTo(0));
    }

    @Test(timeout = 5000L)
    public void telemetry_flushInterval() throws Exception {
        client.telemetry.reset();
        client.telemetry.incrMetricsSent(1);

        assertThat(client.telemetry.metricsSent.get(), equalTo(1));

        // Start flush timer with a 50ms interval
        client.telemetry.start(50L);

        // Wait for the flush to happen
        while (client.telemetry.metricsSent.get() != 0) {
            try {
                Thread.sleep(30L);
            } catch (InterruptedException e) {}
        }
        client.telemetry.stop();

        assertThat(client.telemetry.metricsSent.get(), equalTo(0));

        List<String> statsdMessages = fakeProcessor.getMessagesAsStrings() ;

        assertThat(statsdMessages, hasItem("datadog.dogstatsd.client.metrics:1|c|#test," + telemetryTags + tail));
        assertThat(statsdMessages, hasItem("datadog.dogstatsd.client.events:0|c|#test," + telemetryTags + tail));
        assertThat(statsdMessages, hasItem("datadog.dogstatsd.client.service_checks:0|c|#test," + telemetryTags + tail));
        assertThat(statsdMessages, hasItem("datadog.dogstatsd.client.bytes_sent:0|c|#test," + telemetryTags + tail));
        assertThat(statsdMessages, hasItem("datadog.dogstatsd.client.bytes_dropped:0|c|#test," + telemetryTags + tail));
        assertThat(statsdMessages, hasItem("datadog.dogstatsd.client.packets_sent:0|c|#test," + telemetryTags + tail));
        assertThat(statsdMessages, hasItem("datadog.dogstatsd.client.packets_dropped:0|c|#test," + telemetryTags + tail));
        assertThat(statsdMessages, hasItem("datadog.dogstatsd.client.packets_dropped_queue:0|c|#test," + telemetryTags + tail));
    }

    @Test(timeout = 5000L)
    public void telemetry_droppedData() throws Exception {
        Assume.assumeTrue(TestHelpers.isUdsAvailable());

        // fails to send any data on the network, producing packets dropped
        NonBlockingStatsDClient clientError = new NonBlockingStatsDClientBuilder()
                .prefix("my.prefix")
                .hostname("localhost")
                .constantTags("test")
                .port(0)
                .enableTelemetry(false) // disable telemetry so we can control calls to "flush"
                .originDetectionEnabled(false)
                .containerID(containerID)
                .build();

        assertThat(clientError.statsDProcessor.bufferPool.getBufferSize(), equalTo(8192));

        clientError.gauge("gauge", 24);

        // leaving time to the server to flush metrics
        while (clientError.telemetry.metricsSent.get() == 0
               || clientError.telemetry.packetsDropped.get() == 0
               || clientError.telemetry.bytesDropped.get() == 0) {
            try {
                Thread.sleep(50L);
            } catch (InterruptedException e) {}
        }

        clientError.stop();

        assertThat(clientError.telemetry.metricsSent.get(), equalTo(1));
        assertThat(clientError.telemetry.packetsDropped.get(), equalTo(1));
        assertThat(clientError.telemetry.bytesDropped.get(), equalTo("my.prefix.gauge:24|g|#test".length() + tail.length()));
    }

    @Test(timeout = 5000L)
    public void telemetry_SentData() throws Exception {
        client.telemetry.reset();

        client.gauge("gauge", 24);

        // leaving time to the server to flush metrics (equivalent to waitForMessage)
        while (client.telemetry.metricsSent.get() == 0
               || client.telemetry.packetsSent.get() == 0
               || client.telemetry.bytesSent.get() == 0) {
            try {
                Thread.sleep(50L);
            } catch (InterruptedException e) {}
        }

        assertThat(client.telemetry.metricsSent.get(), equalTo(1));
        assertThat(client.telemetry.packetsSent.get(), equalTo(1));
        assertThat(client.telemetry.bytesSent.get(), equalTo(26 + tail.length()));
    }
}
