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
    protected final CountDownLatch endSignal;
    final CountDownLatch closeSignal;

    protected final ThreadFactory threadFactory;
    protected final Thread[] workers;
    protected final int qcapacity;

    protected StatsDAggregator aggregator;
    protected volatile Telemetry telemetry;

    protected volatile boolean shutdown;
    volatile boolean shutdownAgg;

    String containerID;

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

        protected void processLoop() {
            ByteBuffer sendBuffer;

            try {
                sendBuffer = bufferPool.borrow();
            } catch (final InterruptedException e) {
                return;
            }

            boolean clientClosed = false;
            while (!Thread.interrupted()) {
                try {
                    // Read flags before polling for messages. We can continue shutdown only when
                    // shutdown flag is true *before* we get empty result from the queue. Shadow
                    // the names, so we can't check the non-local copy by accident.
                    boolean shutdown = StatsDProcessor.this.shutdown;
                    boolean shutdownAgg = StatsDProcessor.this.shutdownAgg;

                    Message message = highPrioMessages.poll();
                    if (message == null && shutdownAgg) {
                        break;
                    }
                    if (message == null && !clientClosed) {
                        message = getMessage();
                    }
                    if (message == null) {
                        if (shutdown && !clientClosed) {
                            closeSignal.countDown();
                            clientClosed = true;
                        }
                        if (clientClosed) {
                            // We are draining highPrioMessages, which is a non-blocking
                            // queue. Avoid a busy loop if the queue is empty while the aggregator
                            // is flushing.
                            Thread.sleep(WAIT_SLEEP_MS);
                        }
                        continue;
                    }

                    if (aggregator.aggregateMessage(message)) {
                        continue;
                    }

                    builder.setLength(0);
                    message.writeTo(builder, containerID);
                    int lowerBoundSize = builder.length();

                    if (sendBuffer.capacity() < lowerBoundSize) {
                        throw new InvalidMessageException(MESSAGE_TOO_LONG, builder.toString());
                    }

                    if (sendBuffer.remaining() < (lowerBoundSize + 1)) {
                        outboundQueue.put(sendBuffer);
                        sendBuffer = bufferPool.borrow();
                    }

                    try {
                        writeBuilderToSendBuffer(sendBuffer);
                    } catch (BufferOverflowException boe) {
                        outboundQueue.put(sendBuffer);
                        sendBuffer = bufferPool.borrow();
                        writeBuilderToSendBuffer(sendBuffer);
                    }

                    if (!haveMessages()) {
                        outboundQueue.put(sendBuffer);
                        sendBuffer = bufferPool.borrow();
                    }
                } catch (final InterruptedException e) {
                    break;
                } catch (final Exception e) {
                    handler.handle(e);
                }
            }

            builder.setLength(0);
            builder.trimToSize();
        }

        abstract boolean haveMessages();

        abstract Message getMessage() throws InterruptedException;

        protected void writeBuilderToSendBuffer(ByteBuffer sendBuffer) {

            int length = builder.length();
            // use existing charbuffer if possible, otherwise re-wrap
            if (length <= buffer.capacity()) {
                buffer.limit(length).position(0);
            } else {
                buffer = CharBuffer.wrap(builder);
            }

            sendBuffer.mark();
            if (utf8Encoder.encode(buffer, sendBuffer, true) == CoderResult.OVERFLOW) {
                sendBuffer.reset();
                throw new BufferOverflowException();
            }
        }
    }

    StatsDProcessor(final int queueSize, final StatsDClientErrorHandler handler,
            final int maxPacketSizeBytes, final int poolSize, final int workers,
            final int aggregatorFlushInterval, final int aggregatorShards,
            final ThreadFactory threadFactory, final String containerID) throws Exception {

        this.handler = handler;
        this.threadFactory = threadFactory;
        this.workers = new Thread[workers];
        this.qcapacity = queueSize;

        this.bufferPool = new BufferPool(poolSize, maxPacketSizeBytes, true);
        this.highPrioMessages = new ConcurrentLinkedQueue<>();
        this.outboundQueue = new ArrayBlockingQueue<ByteBuffer>(poolSize);
        this.endSignal = new CountDownLatch(workers);
        this.closeSignal = new CountDownLatch(workers);
        this.aggregator = new StatsDAggregator(this, aggregatorShards, aggregatorFlushInterval);

        this.containerID = containerID;
    }

    protected abstract ProcessingTask createProcessingTask();

    protected abstract boolean send(final Message message);

    protected boolean sendHighPrio(final Message message) {
        highPrioMessages.offer(message);
        return true;
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

    void startWorkers(final String namePrefix) {
        aggregator.start();
        // each task is a busy loop taking up one thread, so keep it simple and use an array of threads
        for (int i = 0 ; i < workers.length ; i++) {
            workers[i] = threadFactory.newThread(createProcessingTask());
            workers[i].setName(namePrefix + (i + 1));
            workers[i].start();
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

    void shutdown(boolean blocking) throws InterruptedException {
        shutdown = true;
        aggregator.stop();

        if (blocking) {
            // Wait for messages to pass through the queues and the aggregator. Shutdown logic for
            // each queue follows the same pattern:
            //
            // if queue returns no messages after a shutdown flag is set, assume no new messages
            // will arrive and count down the latch.
            //
            // This may drop messages if send is called concurrently with shutdown (even in
            // blocking mode); but we will (attempt to) deliver everything that was sent before
            // this point.
            closeSignal.await();
            aggregator.flush();
            shutdownAgg = true;
            endSignal.await();
        } else {
            // Stop all workers immediately.
            for (int i = 0 ; i < workers.length ; i++) {
                workers[i].interrupt();
            }
        }
    }
}
