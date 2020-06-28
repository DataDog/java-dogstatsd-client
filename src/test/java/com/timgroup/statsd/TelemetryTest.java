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

import com.timgroup.statsd.Message;

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
            super(0, handler, 0, 1, 1, 0);
        }


        private class FakeProcessingTask extends StatsDProcessor.ProcessingTask {
            @Override
            public void run() {}
        }

        @Override
        public boolean send(final Message msg) {
            messages.add(msg);
            return true;
        }

        @Override
        public void run(){}

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
                super(prefix, queueSize, constantTags, errorHandler, addressLookup, addressLookup, timeout,
                        bufferSize, maxPacketSizeBytes, entityID, poolSize, processorWorkers, senderWorkers,
                        blocking, enableTelemetry, telemetryFlushInterval, 0);
        }
    };

    private static class StatsDNonBlockingTelemetryBuilder extends NonBlockingStatsDClientBuilder {

        @Override
        public StatsDNonBlockingTelemetry build() throws StatsDClientException {
            if (addressLookup != null) {
                return new StatsDNonBlockingTelemetry(prefix, queueSize, constantTags, errorHandler,
                        addressLookup, timeout, socketBufferSize, maxPacketSizeBytes, entityID,
                        bufferPoolSize, processorWorkers, senderWorkers, blocking, enableTelemetry,
                        telemetryFlushInterval);
            } else {
                return new StatsDNonBlockingTelemetry(prefix, queueSize, constantTags, errorHandler,
                        staticStatsDAddressResolution(hostname, port), timeout, socketBufferSize, maxPacketSizeBytes,
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
        properties.load(TelemetryTest.class.getClassLoader().getResourceAsStream("version.properties"));
        return "client:java," + NonBlockingStatsDClient.CLIENT_VERSION_TAG + properties.getProperty("dogstatsd_client_version") + ",client_transport:udp";
    }

    private static String telemetryTags;

    @BeforeClass
    public static void start() throws IOException, Exception {
        server = new DummyStatsDServer(STATSD_SERVER_PORT);
        fakeProcessor = new FakeProcessor(NO_OP_HANDLER);
        client.telemetry.processor = fakeProcessor;

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
        fakeProcessor.clear();
    }

    @Test(timeout = 5000L)
     public void telemetry_incrManuallyIncrData() throws Exception {
        client.telemetry.incrMetricsSent(1);
        client.telemetry.incrEventsSent(2);
        client.telemetry.incrServiceChecksSent(3);
        client.telemetry.incrBytesSent(4);
        client.telemetry.incrBytesDropped(5);
        client.telemetry.incrPacketSent(6);
        client.telemetry.incrPacketDropped(7);
        client.telemetry.incrPacketDroppedQueue(8);

        assertThat(client.telemetry.metricsSent.get(), equalTo(1));
        assertThat(client.telemetry.eventsSent.get(), equalTo(2));
        assertThat(client.telemetry.serviceChecksSent.get(), equalTo(3));
        assertThat(client.telemetry.bytesSent.get(), equalTo(4));
        assertThat(client.telemetry.bytesDropped.get(), equalTo(5));
        assertThat(client.telemetry.packetsSent.get(), equalTo(6));
        assertThat(client.telemetry.packetsDropped.get(), equalTo(7));
        assertThat(client.telemetry.packetsDroppedQueue.get(), equalTo(8));

        client.telemetry.flush();

        assertThat(client.telemetry.metricsSent.get(), equalTo(0));
        assertThat(client.telemetry.eventsSent.get(), equalTo(0));
        assertThat(client.telemetry.serviceChecksSent.get(), equalTo(0));
        assertThat(client.telemetry.bytesSent.get(), equalTo(0));
        assertThat(client.telemetry.bytesDropped.get(), equalTo(0));
        assertThat(client.telemetry.packetsSent.get(), equalTo(0));
        assertThat(client.telemetry.packetsDropped.get(), equalTo(0));
        assertThat(client.telemetry.packetsDroppedQueue.get(), equalTo(0));

        List<String> statsdMessages = fakeProcessor.getMessagesAsStrings() ;

        assertThat(statsdMessages,
                   hasItem("datadog.dogstatsd.client.metrics:1|c|#test," + telemetryTags));

        assertThat(statsdMessages,
                   hasItem("datadog.dogstatsd.client.events:2|c|#test," + telemetryTags));

        assertThat(statsdMessages,
                   hasItem("datadog.dogstatsd.client.service_checks:3|c|#test," + telemetryTags));

        assertThat(statsdMessages,
                   hasItem("datadog.dogstatsd.client.bytes_sent:4|c|#test," + telemetryTags));

        assertThat(statsdMessages,
                   hasItem("datadog.dogstatsd.client.bytes_dropped:5|c|#test," + telemetryTags));

        assertThat(statsdMessages,
                   hasItem("datadog.dogstatsd.client.packets_sent:6|c|#test," + telemetryTags));

        assertThat(statsdMessages,
                   hasItem("datadog.dogstatsd.client.packets_dropped:7|c|#test," + telemetryTags));

        assertThat(statsdMessages,
                   hasItem("datadog.dogstatsd.client.packets_dropped_queue:8|c|#test," + telemetryTags));
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
                   hasItem("datadog.dogstatsd.client.metrics:1|c|#test," + telemetryTags));

        assertThat(statsdMessages,
                   hasItem("datadog.dogstatsd.client.events:0|c|#test," + telemetryTags));

        assertThat(statsdMessages,
                   hasItem("datadog.dogstatsd.client.service_checks:0|c|#test," + telemetryTags));

        assertThat(statsdMessages,
                   hasItem("datadog.dogstatsd.client.bytes_sent:28|c|#test," + telemetryTags));

        assertThat(statsdMessages,
                   hasItem("datadog.dogstatsd.client.bytes_dropped:0|c|#test," + telemetryTags));

        assertThat(statsdMessages,
                   hasItem("datadog.dogstatsd.client.packets_sent:1|c|#test," + telemetryTags));

        assertThat(statsdMessages,
                   hasItem("datadog.dogstatsd.client.packets_dropped:0|c|#test," + telemetryTags));

        assertThat(statsdMessages,
                   hasItem("datadog.dogstatsd.client.packets_dropped_queue:0|c|#test," + telemetryTags));
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

        assertThat(statsdMessages, hasItem("datadog.dogstatsd.client.metrics:1|c|#test," + telemetryTags));
        assertThat(statsdMessages, hasItem("datadog.dogstatsd.client.events:0|c|#test," + telemetryTags));
        assertThat(statsdMessages, hasItem("datadog.dogstatsd.client.service_checks:0|c|#test," + telemetryTags));
        assertThat(statsdMessages, hasItem("datadog.dogstatsd.client.bytes_sent:0|c|#test," + telemetryTags));
        assertThat(statsdMessages, hasItem("datadog.dogstatsd.client.bytes_dropped:0|c|#test," + telemetryTags));
        assertThat(statsdMessages, hasItem("datadog.dogstatsd.client.packets_sent:0|c|#test," + telemetryTags));
        assertThat(statsdMessages, hasItem("datadog.dogstatsd.client.packets_dropped:0|c|#test," + telemetryTags));
        assertThat(statsdMessages, hasItem("datadog.dogstatsd.client.packets_dropped_queue:0|c|#test," + telemetryTags));
    }

    @Test(timeout = 5000L)
    public void telemetry_droppedData() throws Exception {
        clientError.telemetry.reset();

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
        assertThat(clientError.telemetry.bytesDropped.get(), equalTo(26));
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
        assertThat(client.telemetry.bytesSent.get(), equalTo(26));
    }
}
