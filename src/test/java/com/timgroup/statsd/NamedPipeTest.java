package com.timgroup.statsd;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.nullValue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Random;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;

public class NamedPipeTest implements StatsDClientErrorHandler {
    private static final Logger log = Logger.getLogger("NamedPipeTest");

    private static final Random random = new Random();
    private NonBlockingStatsDClient client;
    private DummyStatsDServer server;
    private volatile Exception lastException = new Exception();

    @Rule public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

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
        client =
                new NonBlockingStatsDClientBuilder()
                        .withEnvironmentVariables(new HashMap<String, String>())
                        .prefix("my.prefix")
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
        for (long i = 0; i < 5; i++) {
            client.gauge("mycount", i);
            server.waitForMessage();
            String expected = String.format("my.prefix.mycount:%d|g", i);
            assertThat(server.messagesReceived(), contains(expected));
            server.clear();
        }
        assertThat(lastException.getMessage(), nullValue());
    }

    @Test(timeout = 5000L)
    public void sends_to_pipe_created_after_build() throws Exception {
        String pipeName = "testPipe-late-" + random.nextInt(10000);
        NonBlockingStatsDClient lateClient = newClient(pipeName, new RecordingErrorHandler());
        DummyStatsDServer lateServer = null;
        try {
            lateServer = new NamedPipeDummyStatsDServer(pipeName);
            lateClient.gauge("late", 1);
            lateServer.waitForMessage();
            assertThat(lateServer.messagesReceived(), contains("my.prefix.late:1|g"));
        } finally {
            lateClient.stop();
            if (lateServer != null) {
                lateServer.close();
            }
        }
    }

    @Test(timeout = 10000L)
    public void reconnects_after_server_restart() throws Exception {
        String pipeName = "testPipe-restart-" + random.nextInt(10000);
        RecordingErrorHandler errorHandler = new RecordingErrorHandler();
        DummyStatsDServer firstServer = new NamedPipeDummyStatsDServer(pipeName);
        NonBlockingStatsDClient restartClient = newClient(pipeName, errorHandler);
        DummyStatsDServer secondServer = null;
        try {
            restartClient.gauge("before", 1);
            firstServer.waitForMessage();
            firstServer.close();

            // The server closes its pipe after its next read, so send until the client sees the
            // broken pipe and closes its end.
            while (errorHandler.getExceptions().isEmpty()) {
                restartClient.gauge("during", 1);
                Thread.sleep(10);
            }

            secondServer = new NamedPipeDummyStatsDServer(pipeName);
            // The queue holds one message, so keep sending until one arrives.
            while (!secondServer.messagesReceived().contains("my.prefix.after:1|g")) {
                restartClient.gauge("after", 1);
                Thread.sleep(10);
            }
        } finally {
            restartClient.stop();
            firstServer.close();
            if (secondServer != null) {
                secondServer.close();
            }
        }
    }

    private static NonBlockingStatsDClient newClient(
            String pipeName, StatsDClientErrorHandler errorHandler) {
        return new NonBlockingStatsDClientBuilder()
                .withEnvironmentVariables(new HashMap<String, String>())
                .prefix("my.prefix")
                .namedPipe(pipeName)
                .queueSize(1)
                .enableAggregation(false)
                .errorHandler(errorHandler)
                .build();
    }
}
