package com.timgroup.statsd;


import java.io.IOException;
import java.net.SocketException;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
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

        assertEquals(testSize, server.messagesReceived().size());
    }
}
