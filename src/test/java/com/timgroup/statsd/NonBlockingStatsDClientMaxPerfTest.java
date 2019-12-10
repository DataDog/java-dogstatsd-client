package com.timgroup.statsd;


import java.io.IOException;
import java.net.SocketException;
import java.util.Random;
import java.util.logging.Logger;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertNotEquals;

public final class NonBlockingStatsDClientMaxPerfTest {

    private static Logger log = Logger.getLogger("NonBlockingStatsDClientMaxPerfTest");
    private static final int STATSD_SERVER_PORT = 17255;
    private static final int BLAST_DURATION_SECS = 60;  // Duration in secs
    private static final int Q_SIZE = 2048;  // Duration in secs
    private static final Random RAND = new Random();
    private static final NonBlockingStatsDClient client = new NonBlockingStatsDClient(
            "my.prefix", "localhost", STATSD_SERVER_PORT, Q_SIZE);
    private final ExecutorService executor = Executors.newFixedThreadPool(10);
    private static AtomicBoolean running;
    private static DummyStatsDServer server;

    @BeforeClass
    public static void start() throws IOException {
        server = new DummyStatsDServer(STATSD_SERVER_PORT);
        running = new AtomicBoolean(true);
    }

    @AfterClass
    public static void stop() throws Exception {
        client.stop();
        server.close();
        running.set(false);
    }

    @Test
    public void perf_test() throws Exception {

        for(int i=0 ; i < 10 ; i++) {
            executor.submit(new Runnable() {
                public void run() {
                    while (running.get()) {
                        client.count("mycount", 1);
                    }
                }
            });
        }

        Thread.sleep(BLAST_DURATION_SECS);

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);

        assertNotEquals(0, server.messagesReceived().size());
        log.info("Messages at server: " + server.messagesReceived().size());
    }
}
