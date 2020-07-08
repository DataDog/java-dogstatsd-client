package com.timgroup.statsd;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Rule;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.comparesEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class StatsDAggregatorTest {

    private static final int STATSD_SERVER_PORT = 17254;
    private static DummyStatsDServer server;

    private static Logger log = Logger.getLogger("StatsDAggregatorTest");

    @Rule
    public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

    @BeforeClass
    public static void start() throws IOException {
        server = new DummyStatsDServer(STATSD_SERVER_PORT);
    }

    @AfterClass
    public static void stop() {
        try {
            server.close();
        } catch (java.io.IOException ignored) {
        }
    }

    @After
    public void clear() {
        server.clear();
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
            .build();

        try {
            for (int i=0 ; i<10 ; i++) {
                testClient.gauge("top.level.value", i);
            }
            server.waitForMessage("my.prefix");

            List<String> messages = server.messagesReceived();
            for (String msg : messages) {
                System.out.println("message: " + msg);
            }

            assertThat(messages.size(), comparesEqualTo(1));
            assertThat(messages, hasItem(comparesEqualTo("my.prefix.top.level.value:9|g")));

        } finally {
            testClient.stop();
        }
    }
}
