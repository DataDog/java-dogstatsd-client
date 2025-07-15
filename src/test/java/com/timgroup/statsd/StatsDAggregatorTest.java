package com.timgroup.statsd;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class StatsDAggregatorTest {

    private static final String TEST_NAME = "StatsDAggregatorTest";
    private FakeProcessor fakeProcessor;

    private static Logger log = Logger.getLogger(TEST_NAME);

    private static final ExecutorService executor =
            Executors.newFixedThreadPool(
                    2,
                    new ThreadFactory() {
                        final ThreadFactory delegate = Executors.defaultThreadFactory();

                        @Override
                        public Thread newThread(final Runnable runnable) {
                            final Thread result = delegate.newThread(runnable);
                            result.setName(TEST_NAME + result.getName());
                            result.setDaemon(true);
                            return result;
                        }
                    });

    @Rule public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

    private static final StatsDClientErrorHandler NO_OP_HANDLER =
            new StatsDClientErrorHandler() {
                @Override
                public void handle(final Exception ex) {
                    /* No-op */
                }
            };

    public static class FakeMessage<T extends Number> extends NumericMessage<T> {
        protected FakeMessage(String aspect, Message.Type type, T value) {
            super(aspect, type, value, TagsCardinality.DEFAULT, null);
        }

        protected FakeMessage(
                String aspect, Message.Type type, T value, TagsCardinality cardinality) {
            super(aspect, type, value, cardinality, null);
        }

        @Override
        protected boolean writeTo(StringBuilder builder, int capacity) {
            return false;
        }
    }

    public static class FakeAlphaMessage extends AlphaNumericMessage {
        protected FakeAlphaMessage(String aspect, Message.Type type, String value) {
            super(aspect, type, value, TagsCardinality.DEFAULT, null);
        }

        @Override
        protected boolean writeTo(StringBuilder builder, int capacity) {
            return false;
        }
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

                        try {
                            Thread.sleep(50L);
                        } catch (InterruptedException e) {
                        }

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
            Message getMessage() {
                return null;
            }

            @Override
            boolean haveMessages() {
                return false;
            }
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
            } catch (Exception e) {
            }
        }
    }

    @Before
    public void start() throws Exception {
        fakeProcessor = new FakeProcessor(NO_OP_HANDLER);

        // 15s flush period should be enough for all tests to be done - flushes will be manual
        StatsDAggregator aggregator =
                new StatsDAggregator(fakeProcessor, StatsDAggregator.DEFAULT_SHARDS, 3000L);
        fakeProcessor.aggregator = aggregator;
        fakeProcessor.startWorkers("StatsD-Test-");
    }

    @After
    public void stop() {
        try {
            fakeProcessor.shutdown(false);
        } catch (InterruptedException e) {
            return;
        }
    }

    public void waitForQueueSize(Queue queue, int size) {

        // Wait for the flush to happen
        while (queue.size() != size) {
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
            }
        }
    }

    @Test(timeout = 2000L)
    public void aggregate_messages() throws Exception {

        for (int i = 0; i < 10; i++) {
            fakeProcessor.send(new FakeMessage<Integer>("some.gauge", Message.Type.GAUGE, 1));
            fakeProcessor.send(new FakeMessage<Integer>("some.count", Message.Type.COUNT, 1));
            fakeProcessor.send(
                    new FakeMessage<Integer>("some.histogram", Message.Type.HISTOGRAM, 1));
            fakeProcessor.send(
                    new FakeMessage<Integer>("some.distribution", Message.Type.DISTRIBUTION, 1));
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
    public void aggregate_messages_with_cardinality() throws Exception {
        for (int i = 0; i < 10; i++) {
            fakeProcessor.send(new FakeMessage<Integer>("some.count", Message.Type.COUNT, 1));
            fakeProcessor.send(
                    new FakeMessage<Integer>(
                            "some.count", Message.Type.COUNT, 1, TagsCardinality.LOW));
            fakeProcessor.send(
                    new FakeMessage<Integer>(
                            "some.count", Message.Type.COUNT, 1, TagsCardinality.HIGH));
        }

        waitForQueueSize(fakeProcessor.messages, 0);

        assertEquals(30, fakeProcessor.messageAggregated.get());
        assertEquals(0, fakeProcessor.messageSent.get());

        fakeProcessor.aggregator.flush();

        assertEquals(3, fakeProcessor.highPrioMessages.size());
    }

    @Test(timeout = 2000L)
    public void aggregation_sharding() throws Exception {
        final int iterations = 10;

        for (int i = 0; i < StatsDAggregator.DEFAULT_SHARDS * iterations; i++) {
            final int hash = i + 1;
            FakeMessage<Integer> gauge =
                    new FakeMessage<Integer>("some.gauge", Message.Type.GAUGE, 1) {
                        @Override
                        public int hashCode() {
                            return hash;
                        }
                    };
            fakeProcessor.send(gauge);
        }

        waitForQueueSize(fakeProcessor.messages, 0);

        for (int i = 0; i < StatsDAggregator.DEFAULT_SHARDS; i++) {
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

        for (int i = 0; i < 10; i++) {
            fakeProcessor.send(new FakeMessage<>("some.gauge", Message.Type.GAUGE, i));
        }

        // processor should auto-flush within 2s
        waitForQueueSize(fakeProcessor.highPrioMessages, 1);

        // aggregated message should take last value -  10
        NumericMessage message = (NumericMessage) fakeProcessor.highPrioMessages.element();
        assertEquals(9, message.getValue());
    }

    @Test(timeout = 5000L)
    public void test_aggregation_degradation_to_treenodes() {
        fakeProcessor.aggregator.flush();
        fakeProcessor.clear();
        // these counts have been determined to trigger treeification of the message aggregates
        int numMessages = 10000;
        int numTags = 100;
        Assume.assumeTrue("assertions depend on divisibility", numMessages % numTags == 0);
        String[][] tags = new String[numTags][];
        for (int i = 0; i < numTags; i++) {
            tags[i] = new String[] {String.valueOf(i)};
        }
        for (int i = 0; i < numMessages; i++) {
            fakeProcessor.send(
                    new NumericMessage<Integer>(
                            "some.counter",
                            Message.Type.COUNT,
                            1,
                            TagsCardinality.DEFAULT,
                            tags[i % numTags]) {
                        @Override
                        boolean writeTo(StringBuilder builder, int capacity) {
                            return false;
                        }

                        @Override
                        public int hashCode() {
                            // provoke collisions
                            return 0;
                        }
                    });
        }
        waitForQueueSize(fakeProcessor.messages, 0);
        fakeProcessor.aggregator.flush();
        assertEquals(numTags, fakeProcessor.highPrioMessages.size());
        for (int i = 0; i < numTags; i++) {
            Message message = fakeProcessor.highPrioMessages.poll();
            assertTrue(message instanceof NumericMessage);
            assertEquals(
                    numMessages / numTags, ((NumericMessage<Integer>) message).value.intValue());
        }
    }
}
