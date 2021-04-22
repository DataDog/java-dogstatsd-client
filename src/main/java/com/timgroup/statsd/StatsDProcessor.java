package com.timgroup.statsd;

import com.timgroup.statsd.Message;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class StatsDProcessor {
    protected static final Charset MESSAGE_CHARSET = Charset.forName("UTF-8");

    protected static final String MESSAGE_TOO_LONG = "Message longer than size of sendBuffer";
    protected static final int WAIT_SLEEP_MS = 10;  // 10 ms would be a 100HZ slice

    protected final StatsDClientErrorHandler handler;

    protected final BufferPool bufferPool;
    protected final Queue<Message> highPrioMessages; // FIFO queue for high priority messages
    protected final BlockingQueue<ByteBuffer> outboundQueue; // FIFO queue with outbound buffers
    protected final ExecutorService executor;
    protected final CountDownLatch endSignal;

    protected final int workers;
    protected final int qcapacity;

    protected StatsDAggregator aggregator;
    protected volatile Telemetry telemetry;

    protected volatile boolean shutdown;

    protected abstract class ProcessingTask implements Runnable {
        protected StringBuilder builder = new StringBuilder();
        protected CharBuffer buffer = CharBuffer.wrap(builder);
        protected final CharsetEncoder utf8Encoder = MESSAGE_CHARSET.newEncoder()
                .onMalformedInput(CodingErrorAction.REPLACE)
                .onUnmappableCharacter(CodingErrorAction.REPLACE);

        public final void run() {
            try {
                processLoop();
            } finally {
                endSignal.countDown();
            }
        }

        protected abstract void processLoop();

        protected void writeBuilderToSendBuffer(ByteBuffer sendBuffer) {

            int length = builder.length();
            // use existing charbuffer if possible, otherwise re-wrap
            if (length <= buffer.capacity()) {
                buffer.limit(length).position(0);
            } else {
                buffer = CharBuffer.wrap(builder);
            }

            if (utf8Encoder.encode(buffer, sendBuffer, true) == CoderResult.OVERFLOW) {
                throw new BufferOverflowException();
            }
        }
    }

    StatsDProcessor(final int queueSize, final StatsDClientErrorHandler handler,
            final int maxPacketSizeBytes, final int poolSize, final int workers,
            final int aggregatorFlushInterval, final int aggregatorShards,
            final ThreadFactory threadFactory) throws Exception {

        this.handler = handler;
        this.workers = workers;
        this.qcapacity = queueSize;

        this.executor = Executors.newFixedThreadPool(workers, threadFactory);

        this.bufferPool = new BufferPool(poolSize, maxPacketSizeBytes, true);
        this.highPrioMessages = new ConcurrentLinkedQueue<>();
        this.outboundQueue = new ArrayBlockingQueue<ByteBuffer>(poolSize);
        this.endSignal = new CountDownLatch(workers);
        this.aggregator = new StatsDAggregator(this, aggregatorShards, aggregatorFlushInterval);
    }

    protected abstract ProcessingTask createProcessingTask();

    protected abstract boolean send(final Message message);

    protected boolean sendHighPrio(final Message message) {
        if (!shutdown) {
            // TODO: unbounded for now...
            highPrioMessages.offer(message);
            return true;
        }

        return false;
    }

    public BufferPool getBufferPool() {
        return this.bufferPool;
    }

    public BlockingQueue<ByteBuffer> getOutboundQueue() {
        return this.outboundQueue;
    }

    public int getQcapacity() {
        return this.qcapacity;
    }

    void startWorkers() {
        aggregator.start();
        for (int i = 0 ; i < workers ; i++) {
            executor.submit(createProcessingTask());
        }
    }

    public StatsDAggregator getAggregator() {
        return this.aggregator;
    }

    public void setTelemetry(final Telemetry telemetry) {
        this.telemetry = telemetry;
    }

    public Telemetry getTelemetry() {
        return telemetry;
    }

    void shutdown() {
        shutdown = true;
        aggregator.stop();
        executor.shutdown();
    }

    boolean awaitUntil(final long deadline) {
        while (true) {
            long remaining = deadline - System.currentTimeMillis();
            try {
                boolean terminated = endSignal.await(remaining, TimeUnit.MILLISECONDS);
                if (!terminated) {
                    executor.shutdownNow();
                }
                return terminated;
            } catch (InterruptedException e) {
                // check again...
            }
        }
    }
}
