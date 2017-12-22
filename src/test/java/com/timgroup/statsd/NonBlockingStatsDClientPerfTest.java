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
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public final class NonBlockingStatsDClientPerfTest {


    private static final int STATSD_SERVER_PORT = 17255;
    private static final Random RAND = new Random();
    public static final String LOCALHOST = "localhost";
    public static final String PREFIX = "my.prefix";
    private final static NonBlockingStatsDClient nonBlockingClient = new NonBlockingStatsDClient(
        PREFIX, LOCALHOST, STATSD_SERVER_PORT);
    private final static BlockingStatsDClient blockingClient = new BlockingStatsDClient(PREFIX,
        LOCALHOST, STATSD_SERVER_PORT, false);

    private final static ConcurrentStatsDClient concurrentClient = new ConcurrentStatsDClient(
        PREFIX, LOCALHOST, STATSD_SERVER_PORT, 100);

    @Parameters(name="{0}")
    public static Iterable<? extends StatsDClient> createClient() {
        DefaultStatsDClient synchronizedBlockingClient = new SynchronizedBlockingStatsDClient();
        return Arrays.asList(nonBlockingClient, synchronizedBlockingClient, concurrentClient);
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
        server.close();
    }

    @After
    public void clearServer() {
        executor.shutdownNow();
        client.stop();
        server.clear();
    }

    @Test(timeout=30000)
    public void perf_test() throws Exception {
        int testSize = 10000;
        for(int i = 0; i < testSize; ++i) {
            executor.submit(new Runnable() {
                public void run() {
                client.count("mycount", RAND.nextInt());
                }
            });
        }

        executor.shutdown();
        executor.awaitTermination(20, TimeUnit.SECONDS);
        // Stop the client and ensure all IO is done. This is mostly for the blocking client,
        // since autoflush is set to false.
        client.stop();
        for(int i = 0; i < 20000 && server.messagesReceived().size() < testSize; i += 50) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException ex) {}
        }
        // Accept 1% error rate due to UDP packet loss. If the previous loop completes (20s), but
        // this passes, this means there was some packet loss.
        assertTrue(server.messagesReceived().size() >= testSize - (testSize/100));
    }

    private static class SynchronizedBlockingStatsDClient extends DefaultStatsDClient {

        public SynchronizedBlockingStatsDClient() {
            super(NonBlockingStatsDClientPerfTest.PREFIX, null, null);
        }

        @Override
        protected synchronized void send(String message) {
            blockingClient.send(message);
        }

        @Override
        public synchronized void stop() {
            blockingClient.stop();
            super.stop();
        }
    }
}
