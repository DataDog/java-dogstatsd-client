package com.timgroup.statsd;

import java.util.concurrent.LinkedBlockingQueue;
import java.io.IOException;
import java.net.SocketException;
import java.util.Random;
import java.util.logging.Logger;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.BlockingQueue;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public final class NonBlockingStatsDClientMaxPerfTest {

    private static Logger log = Logger.getLogger("NonBlockingStatsDClientMaxPerfTest");
    private static final int STATSD_SERVER_PORT = 17255;
     private static final int BLAST_DURATION_SECS = 30;  // Duration in secs  
    private static final int Q_SIZE = 2048;  // Duration in secs
    private static final Random RAND = new Random();
 
    @Test
    public void perf_test() throws Exception {

        for (int size = 64; size < 10 * 1000; size *= 2) {
            for (boolean useOld: new boolean[]{false, true}) {
                final NonBlockingStatsDClient client = new NonBlockingStatsDClient(
            "my.prefix", "localhost", STATSD_SERVER_PORT, size, useOld);
        
                RunTest(client, size, useOld);
                client.stop();
            }
        }
    }

    public void RunTest(final NonBlockingStatsDClient client, int size, boolean useOld) throws Exception {
        final DummyStatsDServer server = new DummyStatsDServer(STATSD_SERVER_PORT);        

        final ExecutorService executor = Executors.newFixedThreadPool(10);
        final AtomicBoolean running = new AtomicBoolean(true);

        for(int i=0 ; i < 10 ; i++) {
            executor.submit(new Runnable() {
                public void run() {
                    while (running.get()) {
                        client.count("mycount", 1);
                    }
                }
            });
        }

        Thread.sleep(TimeUnit.SECONDS.toMillis(BLAST_DURATION_SECS));        
        running.set(false);

        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.SECONDS);

        //assertNotEquals(0, server.messagesReceived().size());
        log.info("Messages at server: " + server.messagesReceived().size() + " " + size + " " + useOld);
        server.close();
    }    
}
