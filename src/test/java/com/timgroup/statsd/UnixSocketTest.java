package com.timgroup.statsd;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;
import java.io.File;
import java.nio.file.Files;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;

public class UnixSocketTest implements StatsDClientErrorHandler {
    private static File tmpFolder;
    private static NonBlockingStatsDClient client;
    private static DummyStatsDServer server;
    private static File socketFile;
    private volatile Exception lastException = new Exception();

    public void handle(Exception exception) {
        lastException = exception;
    }

    @Before
    public void start() throws IOException {
        tmpFolder = Files.createTempDirectory(System.getProperty("java-dsd-test")).toFile();
        tmpFolder.deleteOnExit();
        socketFile = new File(tmpFolder, "socket.sock");
        socketFile.deleteOnExit();

        server = new DummyStatsDServer(socketFile.toString());
        client = new NonBlockingStatsDClient("my.prefix", socketFile.toString(), 0, 1, null, this);
    }

    @After
    public void stop() throws Exception {
        client.stop();
        server.close();
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
        assertThat(lastException.getMessage(), containsString("Connection refused"));

        // Delete the socket file, client should throw an IOException
        lastException = new Exception();
        socketFile.delete();
        while(lastException.getMessage() == null) {
            client.gauge("mycount", 20);
            Thread.sleep(10);
        }
        assertThat(lastException.getMessage(), containsString("No such file or directory"));

        // Re-open the server, next send should work OK
        lastException = new Exception();
        DummyStatsDServer server2 = new DummyStatsDServer(socketFile.toString());

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
        while(lastException.getMessage() == null) {
            client.gauge("mycount", 20);
            Thread.sleep(10);  // We need to fill the buffer, setting a shorter sleep
        }
        assertThat(lastException.getMessage(), containsString("Resource temporarily unavailable"));

        // Make sure we recover after we resume listening
        server.clear();
        server.unfreeze();
        while (!server.messagesReceived().contains("my.prefix.mycount:30|g")) {
            server.clear();
            client.gauge("mycount", 30);
            server.waitForMessage();
        }
        assertThat(server.messagesReceived(), hasItem("my.prefix.mycount:30|g"));
        server.clear();
    }
}
