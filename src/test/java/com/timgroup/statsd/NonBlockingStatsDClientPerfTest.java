package com.timgroup.statsd;


import java.net.SocketException;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public final class NonBlockingStatsDClientPerfTest {


    private static final int STATSD_SERVER_PORT = 17255;
    private static final Random RAND = new Random();
    public static final String LOCALHOST = "localhost";
    public static final String PREFIX = "my.prefix";
    private final static NonBlockingStatsDClient nonBlockingClient = new NonBlockingStatsDClient(
        PREFIX, LOCALHOST, STATSD_SERVER_PORT);
    private final static BlockingStatsDClient blockingClient = new BlockingStatsDClient(PREFIX,
        LOCALHOST, STATSD_SERVER_PORT);
    private final static ConcurrentStatsDClient concurrentClient = new ConcurrentStatsDClient(
        PREFIX, LOCALHOST, STATSD_SERVER_PORT);

    @Parameters(name="{0}")
    public static Iterable<? extends StatsDClient> createClient() {
        return Arrays.asList(nonBlockingClient, blockingClient, concurrentClient);
    }

    @Parameter
    public StatsDClient client;

    private final ExecutorService executor = Executors.newFixedThreadPool(20);
    private static DummyStatsDServer server;

    @BeforeClass
    public static void start() throws SocketException {
        server = new DummyStatsDServer(STATSD_SERVER_PORT);
    }

    @AfterClass
    public static void stop() throws Exception {
        nonBlockingClient.stop();
        blockingClient.stop();
        concurrentClient.stop();
        server.close();
    }

    @After
    public void clearServer() {
        server.clear();
    }

    @Test(timeout=30000)
    public void perf_test() throws Exception {
        int testSize = 1000;
        for(int i = 0; i < testSize; ++i) {
            executor.submit(new Runnable() {
                public void run() {
                    client.count("mycount", RAND.nextInt());
                }
            });

        }
        executor.shutdown();
        executor.awaitTermination(20, TimeUnit.SECONDS);

        for(int i = 0; i < 20000 && server.messagesReceived().size() < testSize; i += 50) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException ex) {}
        }
        assertEquals(testSize, server.messagesReceived().size());
    }
}
