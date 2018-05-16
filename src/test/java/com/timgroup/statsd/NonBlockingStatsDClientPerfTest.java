package com.timgroup.statsd;


import java.io.IOException;
import java.net.SocketException;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public final class NonBlockingStatsDClientPerfTest {


    private static final int STATSD_SERVER_PORT = 17255;
    private static final Random RAND = new Random();
    private static final NonBlockingStatsDClient client = new NonBlockingStatsDClient("my.prefix", "localhost", STATSD_SERVER_PORT);
    private final ExecutorService executor = Executors.newFixedThreadPool(10);
    private static DummyStatsDServer server;

    @BeforeClass
    public static void start() throws IOException {
        server = new DummyStatsDServer(STATSD_SERVER_PORT);
    }

    @AfterClass
    public static void stop() throws Exception {
        client.stop();
        server.close();
    }

    @Test(timeout=30000)
    public void perf_test() throws Exception {

        int testSize = 10000;
        for(int i = 0; i < testSize; ++i) {
            executor.submit(new Runnable() {
                public void run() {
                    client.count("mycount", 1);
                }
            });

        }

        executor.shutdown();
        executor.awaitTermination(20, TimeUnit.SECONDS);

        while(server.messagesReceived().size() < testSize) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException ex) {}
        }

        assertEquals(testSize, server.messagesReceived().size());
    }
}
