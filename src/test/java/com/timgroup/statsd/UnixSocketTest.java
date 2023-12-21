package com.timgroup.statsd;

import java.net.SocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.logging.Logger;
import jnr.unixsocket.UnixSocketAddress;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import java.io.IOException;
import java.io.File;
import java.nio.file.Files;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class UnixSocketTest implements StatsDClientErrorHandler {
    private final String transport;
    private static File tmpFolder;
    private static NonBlockingStatsDClient client;
    private static NonBlockingStatsDClient clientAggregate;
    private static DummyStatsDServer server;
    private static File socketFile;

    private volatile Exception lastException = new Exception();

    private static Logger log = Logger.getLogger(StatsDClientErrorHandler.class.getName());

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][] {{"unixstream"}, {"unixgram"}});
    }
    public synchronized void handle(Exception exception) {
        log.warning("Got exception: " + exception);
        lastException = exception;
    }

    public UnixSocketTest(String transport) {
        this.transport = transport;
    }

    private static boolean isJnrAvailable() {
        return TestHelpers.isJnrAvailable();
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

        if (transport.equals("unixgram")) {
            server = new UnixDatagramSocketDummyStatsDServer(socketFile.toString());
        } else {
            server = new UnixStreamSocketDummyStatsDServer(socketFile.toString());
        }

        Callable<SocketAddress> addressLookup = new Callable<SocketAddress>() {
            @Override
            public SocketAddress call() throws Exception {
                return new UnixSocketAddressWithTransport(new UnixSocketAddress(socketFile.getPath()), UnixSocketAddressWithTransport.TransportType.fromScheme(transport));
            }
        };

        client = new NonBlockingStatsDClientBuilder().prefix("my.prefix")
            .addressLookup(addressLookup)
            .port(0)
            .queueSize(1)
            .timeout(1)  // non-zero timeout to ensure exception triggered if socket buffer full.
            .connectionTimeout(1)
            .socketBufferSize(1024 * 1024)
            .enableAggregation(false)
            .errorHandler(this)
            .originDetectionEnabled(false)
            .build();

        clientAggregate = new NonBlockingStatsDClientBuilder().prefix("my.prefix")
            .addressLookup(addressLookup)
            .port(0)
            .queueSize(1)
            .timeout(1)  // non-zero timeout to ensure exception triggered if socket buffer full.
            .connectionTimeout(1)
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
        assertEquals(client.statsDProcessor.bufferPool.getBufferSize(), NonBlockingStatsDClient.DEFAULT_UDS_MAX_PACKET_SIZE_BYTES);
    }

    @Test(timeout = 5000L)
    public void sends_to_statsd() throws Exception {
        for(long i = 0; i < 5 ; i++) {
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
        while(lastException.getMessage() == null) {
            client.gauge("mycount", 20);
            Thread.sleep(10);
        }
        // Depending on the state of the client at that point we might get different messages.
        assertThat(lastException.getMessage(), anyOf(containsString("Connection refused"), containsString("Broken pipe")));

        // Delete the socket file, client should throw an IOException
        lastException = new Exception();
        socketFile.delete();

        client.gauge("mycount", 21);
        while(lastException.getMessage() == null) {
            Thread.sleep(10);
        }
        // Depending on the state of the client at that point we might get different messages.
        assertThat(lastException.getMessage(), anyOf(containsString("No such file or directory"), containsString("Connection refused")));

        // Re-open the server, next send should work OK
        DummyStatsDServer server2;
        if (transport.equals("unixgram")) {
            server2 = new UnixDatagramSocketDummyStatsDServer(socketFile.toString());
        } else {
            server2 = new UnixStreamSocketDummyStatsDServer(socketFile.toString());
        }

        client.gauge("mycount", 30);
        server2.waitForMessage();

        // Reset the exception now that the server is there and listening. (otherwise the client could still generate exceptions)
        lastException = new Exception();

        // Check that we get no further exceptions
        client.gauge("mycount", 40);
        server2.waitForMessage();

        assertThat(server2.messagesReceived(), hasItem("my.prefix.mycount:40|g"));
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
        if (transport == "unixgram") {
            while (lastException.getMessage() == null) {
                client.gauge("mycount", 20);
                Thread.sleep(10);  // We need to fill the buffer, setting a shorter sleep
            }
            String excMessage = TestHelpers.isLinux() ? "Resource temporarily unavailable" : "No buffer space available";
            assertThat(lastException.getMessage(), containsString(excMessage));
        } else {
            // We can't realistically fill the buffer on a stream socket because we can't timeout in the middle of packets,
            // so we just send a lot of packets
            for (int i = 0; i < 100; i++) {
                client.gauge("mycount", 20);
            }
        }

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

    @Test(timeout = 10000L)
    public void testConnectionTimeout() throws InterruptedException {
        if (transport != "unixstream") {
            // Connection timeout is not supported for unixgram
            return;
        }

        // Delay the `accept()` on the server
        server.freeze();
        client.gauge("mycount", 10);
        Thread.sleep(10);
        server.unfreeze();

        server.waitForMessage();
        assertThat(server.messagesReceived(), contains("my.prefix.mycount:10|g"));
        server.clear();
        assertThat(lastException.getMessage(), nullValue());

    }
}
