package com.timgroup.statsd;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Rule;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.logging.Logger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class StatsDAggregatorTest {

    private static final String TEST_NAME = "StatsDAggregatorTest";
    private static final int STATSD_SERVER_PORT = 17254;
    private static DummyStatsDServer server;
    private static FakeProcessor fakeProcessor;

    private static Logger log = Logger.getLogger(TEST_NAME);

    private static final ExecutorService executor = Executors.newFixedThreadPool(2, new ThreadFactory() {
        final ThreadFactory delegate = Executors.defaultThreadFactory();
        @Override public Thread newThread(final Runnable runnable) {
            final Thread result = delegate.newThread(runnable);
            result.setName(TEST_NAME + result.getName());
            result.setDaemon(true);
            return result;
        }
    });

    @Rule
    public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

    private static final StatsDClientErrorHandler NO_OP_HANDLER = new StatsDClientErrorHandler() {
        @Override public void handle(final Exception ex) { /* No-op */ }
    };

    public static class FakeMessage<T extends Number> extends NumericMessage<T> {
        protected FakeMessage(String aspect, Message.Type type, T value) {
            super(aspect, type, value, null);
        }

        @Override
        protected void writeTo(StringBuilder builder, String containerID){}
    }

    public static class FakeAlphaMessage extends AlphaNumericMessage {
        protected FakeAlphaMessage(String aspect, Message.Type type, String value) {
            super(aspect, type, value, null);
        }

        @Override
        protected void writeTo(StringBuilder builder, String containerID){}
    }


    // fakeProcessor store messages from the telemetry only
    public static class FakeProcessor extends StatsDProcessor {

        private final Queue<Message> messages;
        private final AtomicInteger messageSent = new AtomicInteger(0);
        private final AtomicInteger messageAggregated = new AtomicInteger(0);

        FakeProcessor(final StatsDClientErrorHandler handler) throws Exception {
            super(0, handler, 0, 1, 1, 0, 0, new StatsDThreadFactory());
            this.messages = new ConcurrentLinkedQueue<>();
        }


        private class FakeProcessingTask extends StatsDProcessor.ProcessingTask {
            @Override
            protected void processLoop() {

                while (!shutdown) {
                    final Message message = messages.poll();
                    if (message == null) {

                        try{
                            Thread.sleep(50L);
                        } catch (InterruptedException e) {}

                        continue;
                    }

                    if (aggregator.aggregateMessage(message)) {
                        messageAggregated.incrementAndGet();
                        continue;
                    }

                    // otherwise just "send" it
                    messageSent.incrementAndGet();
                }
            }

            @Override
            Message getMessage() { return null; }

            @Override
            boolean haveMessages() { return false; }
        }

        @Override
        protected StatsDProcessor.ProcessingTask createProcessingTask() {
            return new FakeProcessingTask();
        }

        @Override
        public boolean send(final Message msg) {
            messages.offer(msg);
            return true;
        }

        public Queue<Message> getMessages() {
            return messages;
        }

        public void clear() {
            try {
                messages.clear();
                highPrioMessages.clear();
            } catch (Exception e) {}
        }
    }

    @BeforeClass
    public static void start() throws Exception {
        fakeProcessor = new FakeProcessor(NO_OP_HANDLER);

        // set telemetry
        Telemetry telemetry = new Telemetry.Builder()
                .processor(fakeProcessor)
                .build();
        fakeProcessor.setTelemetry(telemetry);

        // 15s flush period should be enough for all tests to be done - flushes will be manual
        StatsDAggregator aggregator = new StatsDAggregator(fakeProcessor, StatsDAggregator.DEFAULT_SHARDS, 3000L);
        fakeProcessor.aggregator = aggregator;
        fakeProcessor.startWorkers("StatsD-Test-");
    }

    @AfterClass
    public static void stop() {
        try {
            fakeProcessor.shutdown(false);
        } catch (InterruptedException e) {
            return;
        }
    }

    @After
    public void clear() {
        // we should probably clear all queues
        fakeProcessor.clear();
    }

    public void waitForQueueSize(Queue queue, int size) {

        // Wait for the flush to happen
        while (queue.size() != size) {
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {}
        }
    }

    @Test(timeout = 2000L)
    public void aggregate_messages() throws Exception {

        for(int i=0 ; i<10 ; i++) {
            fakeProcessor.send(new FakeMessage<Integer>("some.gauge", Message.Type.GAUGE, 1));
            fakeProcessor.send(new FakeMessage<Integer>("some.count", Message.Type.COUNT, 1));
            fakeProcessor.send(new FakeMessage<Integer>("some.histogram", Message.Type.HISTOGRAM, 1));
            fakeProcessor.send(new FakeMessage<Integer>("some.distribution", Message.Type.DISTRIBUTION, 1));
            fakeProcessor.send(new FakeAlphaMessage("some.set", Message.Type.SET, "value"));
        }

        waitForQueueSize(fakeProcessor.messages, 0);

        // 10 gauges, 10 counts, 10 sets
        assertEquals(30, fakeProcessor.messageAggregated.get());
        // 10 histogram, 10 distribution
        assertEquals(20, fakeProcessor.messageSent.get());

        // wait for aggregator flush...
        fakeProcessor.aggregator.flush();

        // 2 metrics (gauge, count) + 1 set, so 3 aggregates
        assertEquals(3, fakeProcessor.highPrioMessages.size());

    }

    @Test(timeout = 2000L)
    public void aggregation_sharding() throws Exception {
        final int iterations = 10;

        for(int i=0 ; i<StatsDAggregator.DEFAULT_SHARDS*iterations ; i++) {
            FakeMessage<Integer> gauge = new FakeMessage<>("some.gauge", Message.Type.GAUGE, 1);
            gauge.hash = i+1;
            fakeProcessor.send(gauge);
        }

        waitForQueueSize(fakeProcessor.messages, 0);

        for (int i=0 ; i<StatsDAggregator.DEFAULT_SHARDS ; i++) {
            Map<Message, Message> map = fakeProcessor.aggregator.aggregateMetrics.get(i);
            synchronized (map) {
                Iterator<Map.Entry<Message, Message>> iter = map.entrySet().iterator();
                int count = 0;
                while (iter.hasNext()) {
                    count++;
                    iter.next();
                }

                // sharding should be balanced
                assertEquals(iterations, count);
            }
        }
    }

    @Test(timeout = 5000L)
    public void aggregation_flushing() throws Exception {

        for(int i=0 ; i<10 ; i++) {
            fakeProcessor.send(new FakeMessage<>("some.gauge", Message.Type.GAUGE, i));
        }

        // processor should auto-flush within 2s
        waitForQueueSize(fakeProcessor.highPrioMessages, 1);

        //aggregated message should take last value -  10
        NumericMessage message = (NumericMessage)fakeProcessor.highPrioMessages.element();
        assertEquals(9, message.getValue());

    }
}
