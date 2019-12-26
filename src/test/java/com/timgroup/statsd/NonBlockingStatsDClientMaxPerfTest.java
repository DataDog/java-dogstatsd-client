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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static org.junit.Assert.assertNotEquals;

@RunWith(Parameterized.class)
public final class NonBlockingStatsDClientMaxPerfTest {

    private static final int senderWorkers = 4;
    private final int clientWorkers;
    private final int port;
    private final int duration;  // Duration in secs
    private final int qSize; // Queue length (number of elements)

    private NonBlockingStatsDClient client;
    private DummyStatsDServer server;

    private AtomicBoolean running;
    private final ExecutorService executor;

    private static Logger log = Logger.getLogger("NonBlockingStatsDClientMaxPerfTest");

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                 { 30, 17255, 256, 1 },  // 30 seconds, 17255 port, 256 qSize, 1 worker
                 { 30, 17256, 512, 1 },  // 30 seconds, 17255 port, 512 qSize, 1 worker
                 { 30, 17257, 1024, 1 },  // 30 seconds, 17255 port, 1024 qSize, 1 worker
                 { 30, 17258, 2048, 1 },  // 30 seconds, 17255 port, 2048 qSize, 1 worker
                 { 30, 17259, 4096, 1 },  // 30 seconds, 17255 port, 4096 qSize, 1 worker
                 { 30, 17260, Integer.MAX_VALUE, 1 },  // 30 seconds, 17255 port, MAX_VALUE qSize, 1 worker
                 { 30, 17261, 256, 2 },  // 30 seconds, 17255 port, 256 qSize, 2 workers
                 { 30, 17262, 512, 2 },  // 30 seconds, 17255 port, 512 qSize, 2 workers
                 { 30, 17263, 1024, 2 },  // 30 seconds, 17255 port, 1024 qSize, 2 workers
                 { 30, 17264, 2048, 2 },  // 30 seconds, 17255 port, 2048 qSize, 2 workers
                 { 30, 17265, 4096, 2 },  // 30 seconds, 17255 port, 4096 qSize, 2 workers
                 { 30, 17266, Integer.MAX_VALUE, 2 }  // 30 seconds, 17255 port, MAX_VALUE qSize, 2 workers
           });
    }

    public NonBlockingStatsDClientMaxPerfTest(int duration, int port, int qSize, int workers) throws IOException {
        this.duration = duration;
        this.port = port;
        this.qSize = qSize;
        this.clientWorkers = workers;
        this.client = new NonBlockingStatsDClient("my.prefix", "localhost", port, qSize);
        this.server = new DummyStatsDServer(port);

        this.executor = Executors.newFixedThreadPool(senderWorkers);
        this.running = new AtomicBoolean(true);

    }

    @After
    public void stop() throws Exception {
        client.stop();
        server.close();
    }

    @Test
    public void perfTest() throws Exception {

        for(int i=0 ; i < this.senderWorkers ; i++) {
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

        assertNotEquals(0, server.messagesReceived().size());
        log.info("Messages at server: " + server.messagesReceived().size());
    }
}
