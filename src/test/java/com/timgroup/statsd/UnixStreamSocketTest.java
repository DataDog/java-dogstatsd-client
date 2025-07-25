package com.timgroup.statsd;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class UnixStreamSocketTest implements StatsDClientErrorHandler {
    private static File tmpFolder;
    private static NonBlockingStatsDClient client;
    private static NonBlockingStatsDClient clientAggregate;
    private static DummyStatsDServer server;
    private static File socketFile;

    private volatile Exception lastException = new Exception();

    private static Logger log = Logger.getLogger(StatsDClientErrorHandler.class.getName());

    public synchronized void handle(Exception exception) {
        log.info("Got exception: " + exception);
        lastException = exception;
    }

    @BeforeClass
    public static void supportedOnly() throws IOException {
        Assume.assumeTrue(TestHelpers.isUdsAvailable());
    }

    @Before
    public void start() throws IOException {
        tmpFolder = Files.createTempDirectory(System.getProperty("java-dsd-test")).toFile();
        tmpFolder.deleteOnExit();
        socketFile = new File(tmpFolder, "socket.sock");
        socketFile.deleteOnExit();

        server = new UnixStreamSocketDummyStatsDServer(socketFile.toString());

        client =
                new NonBlockingStatsDClientBuilder()
                        .prefix("my.prefix")
                        .address("unixstream://" + socketFile.getPath())
                        .port(0)
                        .queueSize(1)
                        .timeout(500) // non-zero timeout to ensure exception triggered if socket
                        // buffer full.
                        .connectionTimeout(500)
                        .socketBufferSize(1024 * 1024)
                        .enableAggregation(false)
                        .errorHandler(this)
                        .originDetectionEnabled(false)
                        .build();

        clientAggregate =
                new NonBlockingStatsDClientBuilder()
                        .prefix("my.prefix")
                        .address("unixstream://" + socketFile.getPath())
                        .port(0)
                        .queueSize(1)
                        .timeout(500) // non-zero timeout to ensure exception triggered if socket
                        // buffer full.
                        .connectionTimeout(500)
                        .socketBufferSize(1024 * 1024)
                        .enableAggregation(false)
                        .errorHandler(this)
                        .originDetectionEnabled(false)
                        .build();
    }

    @After
    public void stop() throws Exception {
        client.stop();
        clientAggregate.stop();
        server.close();
    }

    @Test
    public void assert_default_uds_size() throws Exception {
        assertEquals(
                client.statsDProcessor.bufferPool.getBufferSize(),
                NonBlockingStatsDClient.DEFAULT_UDS_MAX_PACKET_SIZE_BYTES);
    }

    @Test(timeout = 5000L)
    public void sends_to_statsd() throws Exception {
        for (long i = 0; i < 5; i++) {
            client.gauge("mycount", i);
            server.waitForMessage();
            String expected = String.format("my.prefix.mycount:%d|g", i);
            assertThat(server.messagesReceived(), contains(expected));
            server.clear();
        }
        assertThat(lastException.getMessage(), nullValue());
    }

    @Test(timeout = 10000L)
    public void resist_dsd_restart() throws Exception {
        // Send one metric, check that it works.
        client.gauge("mycount", 10);
        server.waitForMessage();
        assertThat(server.messagesReceived(), contains("my.prefix.mycount:10|g"));
        server.clear();
        assertThat(lastException.getMessage(), nullValue());

        // Close the server, client should throw an IOException
        server.close();

        client.gauge("mycount", 20);
        while (lastException.getMessage() == null) {
            Thread.sleep(10);
        }
        // Depending on the state of the client at that point we might get different messages.
        assertThat(
                lastException.getMessage(),
                anyOf(containsString("Connection refused"), containsString("Broken pipe")));

        // Delete the socket file, client should throw an IOException
        lastException = new Exception();
        socketFile.delete();

        client.gauge("mycount", 21);
        while (lastException.getMessage() == null) {
            Thread.sleep(10);
        }
        assertThat(lastException.getMessage(), containsString("No such file or directory"));

        // Re-open the server, next send should work OK
        DummyStatsDServer server2;
        server2 = new UnixStreamSocketDummyStatsDServer(socketFile.toString());

        lastException = new Exception();

        client.gauge("mycount", 30);
        server2.waitForMessage();
        assertThat(server2.messagesReceived(), hasItem("my.prefix.mycount:30|g"));

        server2.clear();
        assertThat(lastException.getMessage(), nullValue());
        server2.close();
    }

    @Test(timeout = 10000L)
    public void resist_dsd_timeout() throws Exception {
        client.gauge("mycount", 10);
        server.waitForMessage();
        assertThat(server.messagesReceived(), contains("my.prefix.mycount:10|g"));
        server.clear();
        assertThat(lastException.getMessage(), nullValue());

        // Freeze the server to simulate dsd being overwhelmed
        server.freeze();

        while (lastException.getMessage() == null) {
            client.gauge("mycount", 20);
        }
        String excMessage = "Write timed out";
        assertThat(lastException.getMessage(), containsString(excMessage));

        // Make sure we recover after we resume listening
        server.clear();
        server.unfreeze();

        // Now make sure we can receive gauges with 30
        while (!server.messagesReceived().contains("my.prefix.mycount:30|g")) {
            server.clear();
            client.gauge("mycount", 30);
            server.waitForMessage();
        }
        assertThat(server.messagesReceived(), hasItem("my.prefix.mycount:30|g"));
        server.clear();
    }

    @Test(timeout = 5000L)
    public void stream_uds_has_uds_buffer_size() throws Exception {
        final NonBlockingStatsDClient client =
                new NonBlockingStatsDClientBuilder()
                        .prefix("my.prefix")
                        .address("unixstream:///foo")
                        .containerID("fake-container-id")
                        .build();

        assertEquals(
                client.statsDProcessor.bufferPool.getBufferSize(),
                NonBlockingStatsDClient.DEFAULT_UDS_MAX_PACKET_SIZE_BYTES);
    }

    @Test(timeout = 5000L)
    public void max_packet_size_override() throws Exception {
        final NonBlockingStatsDClient client =
                new NonBlockingStatsDClientBuilder()
                        .prefix("my.prefix")
                        .address("unixstream:///foo")
                        .containerID("fake-container-id")
                        .maxPacketSizeBytes(576)
                        .build();

        assertEquals(client.statsDProcessor.bufferPool.getBufferSize(), 576);
    }
}
