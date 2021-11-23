package com.timgroup.statsd;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.nullValue;

import java.io.IOException;
import java.util.Random;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class NamedPipeTest implements StatsDClientErrorHandler {
    private static final Logger log = Logger.getLogger("NamedPipeTest");

    private static final Random random = new Random();
    private NonBlockingStatsDClient client;
    private DummyStatsDServer server;
    private volatile Exception lastException = new Exception();

    public synchronized void handle(Exception exception) {
        log.info("Got exception: " + exception.getMessage());
        lastException = exception;
    }

    @BeforeClass
    public static void supportedOnly() {
        Assume.assumeTrue(System.getProperty("os.name").toLowerCase().contains("windows"));
    }

    @Before
    public void start() {
        String pipeName = "testPipe-" + random.nextInt(10000);

        server = new NamedPipeDummyStatsDServer(pipeName);
        client = new NonBlockingStatsDClientBuilder().prefix("my.prefix")
            .namedPipe(pipeName)
            .queueSize(1)
            .enableAggregation(false)
            .errorHandler(this)
            .build();
    }

    @After
    public void stop() throws IOException {
        if (client != null) {
            client.stop();
        }
        if (server != null) {
            server.close();
        }
    }

    @Test(timeout = 5000L)
    public void sends_to_statsd() {
        for(long i = 0; i < 5 ; i++) {
            client.gauge("mycount", i);
            server.waitForMessage();
            String expected = String.format("my.prefix.mycount:%d|g", i);
            assertThat(server.messagesReceived(), contains(expected));
            server.clear();
        }
        assertThat(lastException.getMessage(), nullValue());
    }
}
