package com.timgroup.statsd;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public final class NonBlockingStatsDClientPerfTest {


    private static final Random RAND = new Random();
    public static final String PREFIX = "my.prefix";
    private final static StatsDClient nonBlockingClient;
    private final static StatsDClient blockingClient;
    private final static StatsDClient concurrentClient;
    private static final List<String> messageReceived = Collections.synchronizedList(
        new ArrayList<String>());

    static {
        Protocol listProtocol = new ListProtocol(messageReceived);
        StatsDClientBuilder builder = new StatsDClientBuilder().prefix(PREFIX)
            .customProtocol(listProtocol);
        blockingClient = builder.buildBlocking();
        nonBlockingClient = builder.buildNonBlocking();
        concurrentClient = builder.buildConcurrent(10);
    }

    @Parameters(name="{0}")
    public static Iterable<? extends StatsDClient> createClient() {
        return Arrays.asList(nonBlockingClient, blockingClient, concurrentClient);
    }

    @Parameter
    public StatsDClient client;

    private final ExecutorService executor = Executors.newFixedThreadPool(20);

    @After
    public void clear() {
        executor.shutdownNow();
        client.stop();
        messageReceived.clear();
    }

    @Test(timeout=30000)
    public void perf_test() throws Exception {
        int testSize = 100000;
        for(int i = 0; i < testSize; ++i) {
            executor.submit(new Runnable() {
                public void run() {
                client.count("mycount", RAND.nextInt());
                }
            });
        }

        executor.shutdown();
        executor.awaitTermination(20, TimeUnit.SECONDS);
        for(int i = 0; i < 20000 && messageReceived.size() < testSize; i += 50) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException ex) {}
        }
        assertEquals(testSize, messageReceived.size());
    }

}
