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
import java.util.concurrent.atomic.AtomicInteger;

public abstract class StatsDProcessor implements Runnable {
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

        public abstract void run();

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
            final int aggregatorFlushInterval, final int aggregatorShards)
            throws Exception {

        this.handler = handler;
        this.workers = workers;
        this.qcapacity = queueSize;

        this.executor = Executors.newFixedThreadPool(workers, new ThreadFactory() {
            final ThreadFactory delegate = Executors.defaultThreadFactory();
            @Override
            public Thread newThread(final Runnable runnable) {
                final Thread result = delegate.newThread(runnable);
                result.setName("StatsD-Processor-" + result.getName());
                result.setDaemon(true);
                return result;
            }
        });

        this.bufferPool = new BufferPool(poolSize, maxPacketSizeBytes, true);
        this.highPrioMessages = new ConcurrentLinkedQueue<>();
        this.outboundQueue = new ArrayBlockingQueue<ByteBuffer>(poolSize);
        this.endSignal = new CountDownLatch(workers);
        this.aggregator = new StatsDAggregator(this, aggregatorShards, aggregatorFlushInterval);
    }

    StatsDProcessor(final StatsDProcessor processor)
            throws Exception {

        this.handler = processor.handler;
        this.workers = processor.workers;
        this.qcapacity = processor.getQcapacity();

        this.executor = Executors.newFixedThreadPool(workers, new ThreadFactory() {
            final ThreadFactory delegate = Executors.defaultThreadFactory();
            @Override
            public Thread newThread(final Runnable runnable) {
                final Thread result = delegate.newThread(runnable);
                result.setName("StatsD-Processor-" + result.getName());
                result.setDaemon(true);
                return result;
            }
        });

        this.bufferPool = new BufferPool(processor.bufferPool);
        this.highPrioMessages = new ConcurrentLinkedQueue<>();
        this.outboundQueue = new ArrayBlockingQueue<ByteBuffer>(this.bufferPool.getSize());
        this.endSignal = new CountDownLatch(this.workers);
        this.aggregator = new StatsDAggregator(this, processor.getAggregator().getShardGranularity(),
                processor.getAggregator().getFlushInterval());
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

    @Override
    public void run() {

        for (int i = 0 ; i < workers ; i++) {
            executor.submit(createProcessingTask());
        }

        boolean done = false;
        while (!done) {
            try {
                endSignal.await();
                done = true;
            } catch (final InterruptedException e) {
                // NOTHING
            }
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

    boolean isShutdown() {
        return shutdown;
    }

    void shutdown() {
        shutdown = true;
        executor.shutdown();
    }
}
