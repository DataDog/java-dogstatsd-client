package com.timgroup.statsd;

import java.io.IOException;
import java.net.SocketException;

import java.util.Arrays;
import java.util.Collection;
import java.util.Random;
import java.util.logging.Logger;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static org.junit.Assert.assertNotEquals;

@RunWith(Parameterized.class)
public final class NonBlockingStatsDClientMaxPerfTest {

    private static final int testWorkers = 4;
    private static final int port = 17255;
    private final int processorWorkers;
    private final int senderWorkers;
    private final int duration;  // Duration in secs
    private final int qSize; // Queue length (number of elements)

    private NonBlockingStatsDClient client;
    private DummyLowMemStatsDServer server;

    private AtomicBoolean running;
    private final ExecutorService executor;

    private static Logger log = Logger.getLogger("NonBlockingStatsDClientMaxPerfTest");

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                 { 30, false, 256, 1, 1 },  // 30 seconds, non-blocking, 256 qSize, 1 worker
                 { 30, false, 512, 1, 1 },  // 30 seconds, non-blocking, 512 qSize, 1 worker
                 { 30, false, 1024, 1, 1 },  // 30 seconds, non-blocking, 1024 qSize, 1 worker
                 { 30, false, 2048, 1, 1 },  // 30 seconds, non-blocking, 2048 qSize, 1 worker
                 { 30, false, 4096, 1, 1 },  // 30 seconds, non-blocking, 4096 qSize, 1 worker
                 // { 30, 17260, Integer.MAX_VALUE, 1 },  // 30 seconds, non-blocking, MAX_VALUE qSize, 1 worker
                 { 30, false, 256, 2, 1 },  // 30 seconds, non-blocking, 256 qSize, 2 workers
                 { 30, false, 512, 2, 1 },  // 30 seconds, non-blocking, 512 qSize, 2 workers
                 { 30, false, 1024, 2, 1 },  // 30 seconds, non-blocking, 1024 qSize, 2 workers
                 { 30, false, 2048, 2, 1 },  // 30 seconds, non-blocking, 2048 qSize, 2 workers
                 { 30, false, 4096, 2, 1 },  // 30 seconds, non-blocking, 4096 qSize, 2 workers
                 // // { 30, false, Integer.MAX_VALUE, 2 }  // 30 seconds, non-blocking, MAX_VALUE qSize, 2 workers
                 { 30, false, 256, 1, 2},  // 30 seconds, non-blocking, 256 qSize, 1 sender worker, 2 processor workers
                 { 30, false, 512, 1, 2 },  // 30 seconds, non-blocking, 512 qSize, 1 sender worker, 2 processor workers
                 { 30, false, 1024, 1, 2 },  // 30 seconds, non-blocking, 1024 qSize, 1 sender worker, 2 processor workers
                 { 30, false, 2048, 1, 2 },  // 30 seconds, non-blocking, 2048 qSize, 1 sender worke, 2 processor workers
                 { 30, false, 4096, 1, 2 },  // 30 seconds, non-blocking, 4096 qSize, 1 sender worke, 2 processor workers
                 // // { 30, false, Integer.MAX_VALUE, 1, 2 },  // 30 seconds, non-blocking, MAX_VALUE qSize, 1 worker
                 { 30, false, 256, 2, 2 },  // 30 seconds, non-blocking, 256 qSize, 2 sender workers, 2 processor workers
                 { 30, false, 512, 2, 2 },  // 30 seconds, non-blocking, 512 qSize, 2 sender workers, 2 processor workers
                 { 30, false, 1024, 2, 2 },  // 30 seconds, non-blocking, 1024 qSize, 2 sender workers, 2 processor workers
                 { 30, false, 2048, 2, 2 },  // 30 seconds, non-blocking, 2048 qSize, 2 sender workers, 2 processor workers
                 { 30, false, 4096, 2, 2 },  // 30 seconds, non-blocking, 4096 qSize, 2 sender workers, 2 processor workers
                 // // { 30, false, Integer.MAX_VALUE, 2 }  // 30 seconds, non-blocking, MAX_VALUE qSize, 2 sender workers
                 { 30, true, 256, 1, 1 },  // 30 seconds, non-blocking, 256 qSize, 1 worker
                 { 30, true, 512, 1, 1 },  // 30 seconds, non-blocking, 512 qSize, 1 worker
                 { 30, true, 1024, 1, 1 },  // 30 seconds, non-blocking, 1024 qSize, 1 worker
                 { 30, true, 2048, 1, 1 },  // 30 seconds, non-blocking, 2048 qSize, 1 worker
                 { 30, true, 4096, 1, 1 },  // 30 seconds, non-blocking, 4096 qSize, 1 worker
                 // { 30, 17260, Integer.MAX_VALUE, 1 },  // 30 seconds, non-blocking, MAX_VALUE qSize, 1 worker
                 { 30, true, 256, 2, 1 },  // 30 seconds, non-blocking, 256 qSize, 2 workers
                 { 30, true, 512, 2, 1 },  // 30 seconds, non-blocking, 512 qSize, 2 workers
                 { 30, true, 1024, 2, 1 },  // 30 seconds, non-blocking, 1024 qSize, 2 workers
                 { 30, true, 2048, 2, 1 },  // 30 seconds, non-blocking, 2048 qSize, 2 workers
                 { 30, true, 4096, 2, 1 },  // 30 seconds, non-blocking, 4096 qSize, 2 workers
                 // // { 30, true, Integer.MAX_VALUE, 2 }  // 30 seconds, non-blocking, MAX_VALUE qSize, 2 workers
                 { 30, true, 256, 1, 2},  // 30 seconds, non-blocking, 256 qSize, 1 sender worker, 2 processor workers
                 { 30, true, 512, 1, 2 },  // 30 seconds, non-blocking, 512 qSize, 1 sender worker, 2 processor workers
                 { 30, true, 1024, 1, 2 },  // 30 seconds, non-blocking, 1024 qSize, 1 sender worker, 2 processor workers
                 { 30, true, 2048, 1, 2 },  // 30 seconds, non-blocking, 2048 qSize, 1 sender worke, 2 processor workers
                 { 30, true, 4096, 1, 2 },  // 30 seconds, non-blocking, 4096 qSize, 1 sender worke, 2 processor workers
                 // // { 30, true, Integer.MAX_VALUE, 1, 2 },  // 30 seconds, non-blocking, MAX_VALUE qSize, 1 worker
                 { 30, true, 256, 2, 2 },  // 30 seconds, non-blocking, 256 qSize, 2 sender workers, 2 processor workers
                 { 30, true, 512, 2, 2 },  // 30 seconds, non-blocking, 512 qSize, 2 sender workers, 2 processor workers
                 { 30, true, 1024, 2, 2 },  // 30 seconds, non-blocking, 1024 qSize, 2 sender workers, 2 processor workers
                 { 30, true, 2048, 2, 2 },  // 30 seconds, non-blocking, 2048 qSize, 2 sender workers, 2 processor workers
                 { 30, true, 4096, 2, 2 },  // 30 seconds, non-blocking, 4096 qSize, 2 sender workers, 2 processor workers
                 // // { 30, true, Integer.MAX_VALUE, 2 }  // 30 seconds, non-blocking, MAX_VALUE qSize, 2 sender workers
           });
    }

    public NonBlockingStatsDClientMaxPerfTest(int duration, boolean blocking, int qSize,
            int processorWorkers, int senderWorkers) throws IOException {
        this.duration = duration;
        this.qSize = qSize;
        this.processorWorkers = processorWorkers;
        this.senderWorkers = senderWorkers;
        this.client = new NonBlockingStatsDClientBuilder().prefix("my.prefix")
            .hostname("localhost")
            .port(port)
            .blocking(blocking)
            .queueSize(qSize)
            .senderWorkers(senderWorkers)
            .processorWorkers(processorWorkers)
            .build();
        this.server = new DummyLowMemStatsDServer(port);

        this.executor = Executors.newFixedThreadPool(senderWorkers);
        this.running = new AtomicBoolean(true);

    }

    /**
     * Run with -Dtest_perf=true if you wish the performance tests defined here to run
     */
    @Before
    public void shouldRun() {
        boolean run = false;
        try {
            run = (System.getProperty("test_perf").compareToIgnoreCase("true") == 0);
        } catch (Exception ex) {
            // NADA
        }
        Assume.assumeTrue(run);
    }

    @After
    public void stop() throws Exception {
        client.stop();
        server.close();
    }

    @Test
    public void perfTest() throws Exception {

        for(int i=0 ; i < this.testWorkers ; i++) {
            executor.submit(new Runnable() {
                public void run() {
                    while (running.get()) {
                        client.count("mycount", 1);
                    }
                }
            });
        }

        Thread.sleep(TimeUnit.SECONDS.toMillis(this.duration));
        running.set(false);

        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.SECONDS);

        assertNotEquals(0, server.getMessageCount());
        log.info("Messages at server: " + server.getMessageCount() + " packets: " + server.getPacketCount());
    }
}
