package com.timgroup.statsd;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.concurrent.Callable;
import java.util.List;
import java.util.ArrayList;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class TelemetryTest {
    private static final StatsDClientErrorHandler NO_OP_HANDLER = new StatsDClientErrorHandler() {
        @Override public void handle(final Exception ex) { /* No-op */ }
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
        }

        @Override
        public boolean send(final Message msg) {
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

        protected List<String> getMessagesAsStrings() {
            StringBuilder sb = new StringBuilder();
            ArrayList<String> stringMessages = new ArrayList<>(messages.size());
            for(Message m : messages) {
                sb.setLength(0);
                m.writeTo(sb);
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

    // StatsDNonBlockingTelemetry exposes the telemetry tor the outside
    private static class StatsDNonBlockingTelemetry extends NonBlockingStatsDClient {
        public StatsDNonBlockingTelemetry(final String prefix, final int queueSize, String[] constantTags,
                                       final StatsDClientErrorHandler errorHandler, Callable<SocketAddress> addressLookup,
                                       final int timeout, final int bufferSize, final int maxPacketSizeBytes,
                                       String entityID, final int poolSize, final int processorWorkers,
                                       final int senderWorkers, boolean blocking, final boolean enableTelemetry,
                                       final int telemetryFlushInterval)
                throws StatsDClientException {

            super(new NonBlockingStatsDClientBuilder()
                .prefix(prefix)
                .queueSize(queueSize)
                .constantTags(constantTags)
                .errorHandler(errorHandler)
                .addressLookup(addressLookup)
                .timeout(timeout)
                .socketBufferSize(bufferSize)
                .maxPacketSizeBytes(maxPacketSizeBytes)
                .entityID(entityID)
                .bufferPoolSize(poolSize)
                .processorWorkers(processorWorkers)
                .senderWorkers(senderWorkers)
                .blocking(blocking)
                .enableTelemetry(enableTelemetry)
                .telemetryFlushInterval(telemetryFlushInterval)
                .resolve());
        }
    };

    private static class StatsDNonBlockingTelemetryBuilder extends NonBlockingStatsDClientBuilder {

        @Override
        public StatsDNonBlockingTelemetry build() throws StatsDClientException {

            int packetSize = maxPacketSizeBytes;
            if (packetSize == 0) {
                packetSize = (port == 0) ? NonBlockingStatsDClient.DEFAULT_UDS_MAX_PACKET_SIZE_BYTES :
                    NonBlockingStatsDClient.DEFAULT_UDP_MAX_PACKET_SIZE_BYTES;
            }

            if (addressLookup != null) {
                return new StatsDNonBlockingTelemetry(prefix, queueSize, constantTags, errorHandler,
                        addressLookup, timeout, socketBufferSize, packetSize, entityID,
                        bufferPoolSize, processorWorkers, senderWorkers, blocking, enableTelemetry,
                        telemetryFlushInterval);
            } else {
                return new StatsDNonBlockingTelemetry(prefix, queueSize, constantTags, errorHandler,
                        staticStatsDAddressResolution(hostname, port), timeout, socketBufferSize, packetSize,
                        entityID, bufferPoolSize, processorWorkers, senderWorkers, blocking, enableTelemetry,
                        telemetryFlushInterval);
            }
        }
    }

    private static final int STATSD_SERVER_PORT = 17254;
    private static final NonBlockingStatsDClientBuilder builder = new StatsDNonBlockingTelemetryBuilder()
        .prefix("my.prefix")
        .hostname("localhost")
        .constantTags("test")
        .port(STATSD_SERVER_PORT)
        .enableTelemetry(false); // disable telemetry so we can control calls to "flush"
    private static StatsDNonBlockingTelemetry client = ((StatsDNonBlockingTelemetryBuilder)builder).build();

    // telemetry client
    private static final NonBlockingStatsDClientBuilder telemetryBuilder = new StatsDNonBlockingTelemetryBuilder()
            .prefix("my.prefix")
            .hostname("localhost")
            .constantTags("test")
            .port(STATSD_SERVER_PORT)
            .enableTelemetry(false);  // disable telemetry so we can control calls to "flush"
    private static StatsDNonBlockingTelemetry telemetryClient = ((StatsDNonBlockingTelemetryBuilder)telemetryBuilder).build();

    // builderError fails to send any data on the network, producing packets dropped
    private static final NonBlockingStatsDClientBuilder builderError = new StatsDNonBlockingTelemetryBuilder()
        .prefix("my.prefix")
        .hostname("localhost")
        .constantTags("test")
        .port(0)
        .enableTelemetry(false); // disable telemetry so we can control calls to "flush"
    private static StatsDNonBlockingTelemetry clientError = ((StatsDNonBlockingTelemetryBuilder)builderError).build();

    private static DummyStatsDServer server;
    private static FakeProcessor fakeProcessor;

    private static String computeTelemetryTags() throws IOException, Exception {
        Properties properties = new Properties();
        properties.load(TelemetryTest.class.getClassLoader().getResourceAsStream(
            "dogstatsd/version.properties"));
        return "client:java," + NonBlockingStatsDClient.CLIENT_VERSION_TAG + properties.getProperty("dogstatsd_client_version") + ",client_transport:udp";
    }

    private static String telemetryTags;

    @BeforeClass
    public static void start() throws IOException, Exception {
        server = new DummyStatsDServer(STATSD_SERVER_PORT);
        fakeProcessor = new FakeProcessor(NO_OP_HANDLER);
        client.telemetry.processor = fakeProcessor;
        telemetryClient.telemetry.processor = fakeProcessor;

        telemetryTags = computeTelemetryTags();
    }

    @AfterClass
    public static void stop() throws Exception {
        try {
            client.stop();
            clientError.stop();
            server.close();
        } catch (java.io.IOException e) {
            return;
        }
    }

    @After
    public void clear() {
        server.clear();
        client.telemetry.reset();
        clientError.telemetry.reset();
        telemetryClient.telemetry.reset();
        fakeProcessor.clear();
    }

    @Test(timeout = 5000L)
    public void telemetry_incrManuallyIncrData() throws Exception {

        telemetryClient.telemetry.incrMetricsSent(1);
        telemetryClient.telemetry.incrGaugeSent(1);
        telemetryClient.telemetry.incrCountSent(1);
        telemetryClient.telemetry.incrSetSent(1);
        telemetryClient.telemetry.incrHistogramSent(1);
        telemetryClient.telemetry.incrDistributionSent(1);
        telemetryClient.telemetry.incrMetricsSent(1, Message.Type.GAUGE);  // adds to metricsSent
        telemetryClient.telemetry.incrMetricsSent(1, Message.Type.COUNT);  // adds to metricsSent
        telemetryClient.telemetry.incrMetricsSent(1, Message.Type.SET);  // adds to metricsSent
        telemetryClient.telemetry.incrMetricsSent(1, Message.Type.HISTOGRAM);  // adds to metricsSent
        telemetryClient.telemetry.incrMetricsSent(1, Message.Type.DISTRIBUTION);  // adds to metricsSent
        telemetryClient.telemetry.incrEventsSent(2);
        telemetryClient.telemetry.incrServiceChecksSent(3);
        telemetryClient.telemetry.incrBytesSent(4);
        telemetryClient.telemetry.incrBytesDropped(5);
        telemetryClient.telemetry.incrPacketSent(6);
        telemetryClient.telemetry.incrPacketDropped(7);
        telemetryClient.telemetry.incrPacketDroppedQueue(8);
        telemetryClient.telemetry.incrAggregatedContexts(9);
        telemetryClient.telemetry.incrAggregatedGaugeContexts(10);
        telemetryClient.telemetry.incrAggregatedCountContexts(11);
        telemetryClient.telemetry.incrAggregatedSetContexts(12);

        assertThat(telemetryClient.telemetry.metricsSent.get(), equalTo(6));
        assertThat(telemetryClient.telemetry.gaugeSent.get(), equalTo(2));
        assertThat(telemetryClient.telemetry.countSent.get(), equalTo(2));
        assertThat(telemetryClient.telemetry.setSent.get(), equalTo(2));
        assertThat(telemetryClient.telemetry.histogramSent.get(), equalTo(2));
        assertThat(telemetryClient.telemetry.distributionSent.get(), equalTo(2));
        assertThat(telemetryClient.telemetry.eventsSent.get(), equalTo(2));
        assertThat(telemetryClient.telemetry.serviceChecksSent.get(), equalTo(3));
        assertThat(telemetryClient.telemetry.bytesSent.get(), equalTo(4));
        assertThat(telemetryClient.telemetry.bytesDropped.get(), equalTo(5));
        assertThat(telemetryClient.telemetry.packetsSent.get(), equalTo(6));
        assertThat(telemetryClient.telemetry.packetsDropped.get(), equalTo(7));
        assertThat(telemetryClient.telemetry.packetsDroppedQueue.get(), equalTo(8));
        assertThat(telemetryClient.telemetry.aggregatedContexts.get(), equalTo(9));
        assertThat(telemetryClient.telemetry.aggregatedGaugeContexts.get(), equalTo(10));
        assertThat(telemetryClient.telemetry.aggregatedCountContexts.get(), equalTo(11));
        assertThat(telemetryClient.telemetry.aggregatedSetContexts.get(), equalTo(12));

        telemetryClient.telemetry.flush();

        assertThat(telemetryClient.telemetry.metricsSent.get(), equalTo(0));
        assertThat(telemetryClient.telemetry.gaugeSent.get(), equalTo(0));
        assertThat(telemetryClient.telemetry.countSent.get(), equalTo(0));
        assertThat(telemetryClient.telemetry.setSent.get(), equalTo(0));
        assertThat(telemetryClient.telemetry.histogramSent.get(), equalTo(0));
        assertThat(telemetryClient.telemetry.distributionSent.get(), equalTo(0));
        assertThat(telemetryClient.telemetry.eventsSent.get(), equalTo(0));
        assertThat(telemetryClient.telemetry.serviceChecksSent.get(), equalTo(0));
        assertThat(telemetryClient.telemetry.bytesSent.get(), equalTo(0));
        assertThat(telemetryClient.telemetry.bytesDropped.get(), equalTo(0));
        assertThat(telemetryClient.telemetry.packetsSent.get(), equalTo(0));
        assertThat(telemetryClient.telemetry.packetsDropped.get(), equalTo(0));
        assertThat(telemetryClient.telemetry.packetsDroppedQueue.get(), equalTo(0));
        assertThat(telemetryClient.telemetry.aggregatedContexts.get(), equalTo(0));
        assertThat(telemetryClient.telemetry.aggregatedGaugeContexts.get(), equalTo(0));
        assertThat(telemetryClient.telemetry.aggregatedCountContexts.get(), equalTo(0));
        assertThat(telemetryClient.telemetry.aggregatedSetContexts.get(), equalTo(0));

        List<String> statsdMessages = fakeProcessor.getMessagesAsStrings() ;

        assertThat(statsdMessages,
                hasItem("datadog.dogstatsd.client.metrics:6|c|#test," + telemetryTags + "\n"));

        assertThat(statsdMessages,
                hasItem("datadog.dogstatsd.client.metrics_by_type:2|c|#test," +
                    telemetryClient.telemetry.getTelemetryTags(telemetryTags, Message.Type.GAUGE) + "\n"));

        assertThat(statsdMessages,
                hasItem("datadog.dogstatsd.client.metrics_by_type:2|c|#test," +
                    telemetryClient.telemetry.getTelemetryTags(telemetryTags, Message.Type.COUNT) + "\n"));

        assertThat(statsdMessages,
                hasItem("datadog.dogstatsd.client.metrics_by_type:2|c|#test," +
                    telemetryClient.telemetry.getTelemetryTags(telemetryTags, Message.Type.SET) + "\n"));

        assertThat(statsdMessages,
                hasItem("datadog.dogstatsd.client.metrics_by_type:2|c|#test," +
                    telemetryClient.telemetry.getTelemetryTags(telemetryTags, Message.Type.HISTOGRAM) + "\n"));

        assertThat(statsdMessages,
                hasItem("datadog.dogstatsd.client.metrics_by_type:2|c|#test," +
                    telemetryClient.telemetry.getTelemetryTags(telemetryTags, Message.Type.DISTRIBUTION) + "\n"));

        assertThat(statsdMessages,
                hasItem("datadog.dogstatsd.client.events:2|c|#test," + telemetryTags + "\n"));

        assertThat(statsdMessages,
                hasItem("datadog.dogstatsd.client.service_checks:3|c|#test," + telemetryTags + "\n"));

        assertThat(statsdMessages,
                hasItem("datadog.dogstatsd.client.bytes_sent:4|c|#test," + telemetryTags + "\n"));

        assertThat(statsdMessages,
                hasItem("datadog.dogstatsd.client.bytes_dropped:5|c|#test," + telemetryTags + "\n"));

        assertThat(statsdMessages,
                hasItem("datadog.dogstatsd.client.packets_sent:6|c|#test," + telemetryTags + "\n"));

        assertThat(statsdMessages,
                hasItem("datadog.dogstatsd.client.packets_dropped:7|c|#test," + telemetryTags + "\n"));

        assertThat(statsdMessages,
                hasItem("datadog.dogstatsd.client.packets_dropped_queue:8|c|#test," + telemetryTags + "\n"));

        assertThat(statsdMessages,
                hasItem("datadog.dogstatsd.client.aggregated_context:9|c|#test," + telemetryTags + "\n"));

        assertThat(statsdMessages,
                hasItem("datadog.dogstatsd.client.aggregated_context_by_type:10|c|#test," +
                    telemetryClient.telemetry.getTelemetryTags(telemetryTags, Message.Type.GAUGE) + "\n"));

        assertThat(statsdMessages,
                hasItem("datadog.dogstatsd.client.aggregated_context_by_type:11|c|#test," +
                    telemetryClient.telemetry.getTelemetryTags(telemetryTags, Message.Type.COUNT) + "\n"));

        assertThat(statsdMessages,
                hasItem("datadog.dogstatsd.client.aggregated_context_by_type:12|c|#test," +
                    telemetryClient.telemetry.getTelemetryTags(telemetryTags, Message.Type.SET) + "\n"));
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
                   hasItem("datadog.dogstatsd.client.metrics:1|c|#test," + telemetryTags + "\n"));

        assertThat(statsdMessages,
                   hasItem("datadog.dogstatsd.client.events:0|c|#test," + telemetryTags + "\n"));

        assertThat(statsdMessages,
                   hasItem("datadog.dogstatsd.client.service_checks:0|c|#test," + telemetryTags + "\n"));

        assertThat(statsdMessages,
                   hasItem("datadog.dogstatsd.client.bytes_sent:29|c|#test," + telemetryTags + "\n"));

        assertThat(statsdMessages,
                   hasItem("datadog.dogstatsd.client.bytes_dropped:0|c|#test," + telemetryTags + "\n"));

        assertThat(statsdMessages,
                   hasItem("datadog.dogstatsd.client.packets_sent:1|c|#test," + telemetryTags + "\n"));

        assertThat(statsdMessages,
                   hasItem("datadog.dogstatsd.client.packets_dropped:0|c|#test," + telemetryTags + "\n"));

        assertThat(statsdMessages,
                   hasItem("datadog.dogstatsd.client.packets_dropped_queue:0|c|#test," + telemetryTags + "\n"));

        assertThat(statsdMessages,
                   hasItem("datadog.dogstatsd.client.aggregated_context:0|c|#test," + telemetryTags + "\n"));
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

        assertThat(statsdMessages, hasItem("datadog.dogstatsd.client.metrics:1|c|#test," + telemetryTags + "\n"));
        assertThat(statsdMessages, hasItem("datadog.dogstatsd.client.events:0|c|#test," + telemetryTags + "\n"));
        assertThat(statsdMessages, hasItem("datadog.dogstatsd.client.service_checks:0|c|#test," + telemetryTags + "\n"));
        assertThat(statsdMessages, hasItem("datadog.dogstatsd.client.bytes_sent:0|c|#test," + telemetryTags + "\n"));
        assertThat(statsdMessages, hasItem("datadog.dogstatsd.client.bytes_dropped:0|c|#test," + telemetryTags + "\n"));
        assertThat(statsdMessages, hasItem("datadog.dogstatsd.client.packets_sent:0|c|#test," + telemetryTags + "\n"));
        assertThat(statsdMessages, hasItem("datadog.dogstatsd.client.packets_dropped:0|c|#test," + telemetryTags + "\n"));
        assertThat(statsdMessages, hasItem("datadog.dogstatsd.client.packets_dropped_queue:0|c|#test," + telemetryTags + "\n"));
    }

    @Test(timeout = 5000L)
    public void telemetry_droppedData() throws Exception {
        clientError.telemetry.reset();

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

        assertThat(clientError.telemetry.metricsSent.get(), equalTo(1));
        assertThat(clientError.telemetry.packetsDropped.get(), equalTo(1));
        assertThat(clientError.telemetry.bytesDropped.get(), equalTo(27));
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
        assertThat(client.telemetry.bytesSent.get(), equalTo(27));
    }

    @Test(timeout = 5000L)
    public void telemetry_DevModeData() throws Exception {


        telemetryClient.gauge("gauge", 24);
        telemetryClient.count("count", 1);
        telemetryClient.histogram("histo", 1);
        telemetryClient.distribution("distro", 1);

        // leaving time to the server to flush metrics (equivalent to waitForMessage)
        while (telemetryClient.telemetry.metricsSent.get() == 0
               || telemetryClient.telemetry.packetsSent.get() == 0
               || telemetryClient.telemetry.bytesSent.get() == 0) {
            try {
                Thread.sleep(50L);
            } catch (InterruptedException e) {}
        }

        assertThat(telemetryClient.telemetry.metricsSent.get(), equalTo(4));
        assertThat(telemetryClient.telemetry.gaugeSent.get(), equalTo(1));
        assertThat(telemetryClient.telemetry.countSent.get(), equalTo(1));
        assertThat(telemetryClient.telemetry.setSent.get(), equalTo(0));
        assertThat(telemetryClient.telemetry.histogramSent.get(), equalTo(1));
        assertThat(telemetryClient.telemetry.distributionSent.get(), equalTo(1));
        assertThat(telemetryClient.telemetry.packetsSent.get(), equalTo(1));
        assertThat(telemetryClient.telemetry.bytesSent.get(), equalTo(106));

        // Start flush timer with a 50ms interval
        telemetryClient.telemetry.start(50L);

        // Wait for the flush to happen
        while (telemetryClient.telemetry.metricsSent.get() != 0) {
            try {
                Thread.sleep(30L);
            } catch (InterruptedException e) {}
        }
        telemetryClient.telemetry.stop();

        assertThat(telemetryClient.telemetry.metricsSent.get(), equalTo(0));
        List<String> statsdMessages = fakeProcessor.getMessagesAsStrings();

        assertThat(statsdMessages, hasItem("datadog.dogstatsd.client.metrics:4|c|#test," + telemetryTags + "\n"));
        assertThat(statsdMessages, hasItem("datadog.dogstatsd.client.metrics_by_type:1|c|#test," +
                    telemetryClient.telemetry.getTelemetryTags(telemetryTags, Message.Type.GAUGE) + "\n"));
        assertThat(statsdMessages, hasItem("datadog.dogstatsd.client.metrics_by_type:1|c|#test," +
                    telemetryClient.telemetry.getTelemetryTags(telemetryTags, Message.Type.COUNT) + "\n"));
        assertThat(statsdMessages, hasItem("datadog.dogstatsd.client.metrics_by_type:0|c|#test," +
                    telemetryClient.telemetry.getTelemetryTags(telemetryTags, Message.Type.SET) + "\n"));
        assertThat(statsdMessages, hasItem("datadog.dogstatsd.client.metrics_by_type:1|c|#test," +
                    telemetryClient.telemetry.getTelemetryTags(telemetryTags, Message.Type.HISTOGRAM) + "\n"));
        assertThat(statsdMessages, hasItem("datadog.dogstatsd.client.metrics_by_type:1|c|#test," +
                    telemetryClient.telemetry.getTelemetryTags(telemetryTags, Message.Type.DISTRIBUTION) + "\n"));
        assertThat(statsdMessages, hasItem("datadog.dogstatsd.client.events:0|c|#test," + telemetryTags + "\n"));
        assertThat(statsdMessages, hasItem("datadog.dogstatsd.client.service_checks:0|c|#test," + telemetryTags + "\n"));
        assertThat(statsdMessages, hasItem("datadog.dogstatsd.client.bytes_sent:106|c|#test," + telemetryTags + "\n"));
        assertThat(statsdMessages, hasItem("datadog.dogstatsd.client.bytes_dropped:0|c|#test," + telemetryTags + "\n"));
        assertThat(statsdMessages, hasItem("datadog.dogstatsd.client.packets_sent:1|c|#test," + telemetryTags + "\n"));
        assertThat(statsdMessages, hasItem("datadog.dogstatsd.client.packets_dropped:0|c|#test," + telemetryTags + "\n"));
        assertThat(statsdMessages, hasItem("datadog.dogstatsd.client.packets_dropped_queue:0|c|#test," + telemetryTags + "\n"));
        // aggregation is disabled
        assertThat(statsdMessages, hasItem("datadog.dogstatsd.client.aggregated_context:0|c|#test," + telemetryTags + "\n"));
        assertThat(statsdMessages, hasItem("datadog.dogstatsd.client.aggregated_context_by_type:0|c|#test," +
                    telemetryClient.telemetry.getTelemetryTags(telemetryTags, Message.Type.GAUGE) + "\n"));
        assertThat(statsdMessages, hasItem("datadog.dogstatsd.client.aggregated_context_by_type:0|c|#test," +
                    telemetryClient.telemetry.getTelemetryTags(telemetryTags, Message.Type.COUNT) + "\n"));
        assertThat(statsdMessages, hasItem("datadog.dogstatsd.client.aggregated_context_by_type:0|c|#test," +
                    telemetryClient.telemetry.getTelemetryTags(telemetryTags, Message.Type.SET) + "\n"));
    }
}
