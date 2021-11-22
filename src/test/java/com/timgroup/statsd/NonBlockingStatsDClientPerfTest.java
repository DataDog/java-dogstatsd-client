package com.timgroup.statsd;


import java.io.IOException;
import java.net.SocketException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.Random;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public final class NonBlockingStatsDClientPerfTest {


    private static final int STATSD_SERVER_PORT = 17255;
    private static final Random RAND = new Random();
    private static final NonBlockingStatsDClient client = new NonBlockingStatsDClientBuilder().prefix("my.prefix")
        .hostname("localhost")
        .port(STATSD_SERVER_PORT)
        .blocking(true)  // non-blocking processors will drop messages if the queue fills up
        .enableTelemetry(false)
        .enableAggregation(false)
        .build();

    private static final NonBlockingStatsDClient clientAggr = new NonBlockingStatsDClientBuilder().prefix("my.prefix.aggregated")
        .hostname("localhost")
        .port(STATSD_SERVER_PORT)
        .blocking(true)  // non-blocking processors will drop messages if the queue fills up
        .enableTelemetry(false)
        .build();

    private ExecutorService executor;
    private static DummyStatsDServer server;

    private static Logger log = Logger.getLogger("NonBlockingStatsDClientPerfTest");

    @BeforeClass
    public static void start() throws IOException {
        server = new UDPDummyStatsDServer(STATSD_SERVER_PORT);
    }

    @AfterClass
    public static void stop() throws Exception {

        client.stop();
        clientAggr.stop();
        server.close();
    }

    @Before
    public void setup() throws Exception {
        executor = Executors.newFixedThreadPool(10);
    }

    @After
    public void clear() throws Exception {
        executor.shutdown();
        executor.awaitTermination(20, TimeUnit.SECONDS);
        server.clear();
    }

    @Test(timeout=30000)
    public void perfTest() throws Exception {

        int testSize = 10000;
        for(int i = 0; i < testSize; ++i) {
            executor.submit(new Runnable() {
                public void run() {
                    client.count("mycount", 1);
                }
            });

        }

        while(server.messagesReceived().size() < testSize) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException ex) {}
        }

        assertEquals(testSize, server.messagesReceived().size());
    }

    @Test(timeout=30000)
    public void perfAggregatedTest() throws Exception {

        int expectedSize = 1;
        long elapsed = 0, start = System.currentTimeMillis();
        boolean done = false;

        while(!done) {
            clientAggr.count("myaggrcount", 1);

            elapsed = System.currentTimeMillis() - start;
            if  (elapsed > clientAggr.statsDProcessor.getAggregator().getFlushInterval() - 1) {
                done = true;
            }
            Thread.sleep(50);
        }


        int messages;
        while((messages = server.messagesReceived().size()) < expectedSize) {

            try {
                Thread.sleep(500);
            } catch (InterruptedException ex) {}
        }

        assertEquals(expectedSize, server.messagesReceived().size());
    }
}
