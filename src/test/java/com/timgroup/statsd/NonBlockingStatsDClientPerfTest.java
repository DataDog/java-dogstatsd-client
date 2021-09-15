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
        .enableAggregation(false)
        .build();

    private static final NonBlockingStatsDClient clientAggr = new NonBlockingStatsDClientBuilder().prefix("my.prefix.aggregated")
        .hostname("localhost")
        .port(STATSD_SERVER_PORT)
        .blocking(true)  // non-blocking processors will drop messages if the queue fills up
        .build();

    private final ExecutorService executor = Executors.newFixedThreadPool(10);
    private static DummyStatsDServer server;

    private static Logger log = Logger.getLogger("NonBlockingStatsDClientPerfTest");

    @BeforeClass
    public static void start() throws IOException {
        server = new DummyStatsDServer(STATSD_SERVER_PORT);
    }

    @AfterClass
    public static void stop() throws Exception {
        client.stop();
        server.close();
    }

    @After
    public void clear() throws Exception {
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

        executor.shutdown();
        executor.awaitTermination(20, TimeUnit.SECONDS);

        int messages;
        while((messages = server.messagesReceived().size()) < testSize) {

            log.info("Messages at server: " + messages);
            try {
                Thread.sleep(50);
            } catch (InterruptedException ex) {}
        }

        log.info("Messages at server: " + messages);
        log.info("Packets at server: " + server.packetsReceived());

        assertEquals(testSize, server.messagesReceived().size());
    }

    @Test(timeout=30000)
    public void perfAggregatedTest() throws Exception {

        int expectedSize = 2;
        long start = System.currentTimeMillis();
        boolean done = false;

        while(!done) {
            executor.submit(new Runnable() {
                public void run() {
                    clientAggr.count("mycount", 1);
                }
            });

            long elapsed = System.currentTimeMillis() - start;
            if  (elapsed > clientAggr.statsDProcessor.getAggregator().getFlushInterval()) {
                done = true;
            }
        }

        executor.shutdown();
        executor.awaitTermination(20, TimeUnit.SECONDS);

        int messages;
        while((messages = server.messagesReceived().size()) < expectedSize) {

            log.info("Messages at server: " + messages);
            try {
                Thread.sleep(50);
            } catch (InterruptedException ex) {}
        }

        log.info("Messages at server: " + messages);
        log.info("Packets at server: " + server.packetsReceived());

        assertEquals(expectedSize, server.messagesReceived().size());
    }
}
