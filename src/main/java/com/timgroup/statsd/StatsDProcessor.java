package com.timgroup.statsd;

import com.timgroup.statsd.Message;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class StatsDProcessor {
    protected static final String MESSAGE_TOO_LONG = "Message longer than size of sendBuffer";
    protected static final int WAIT_SLEEP_MS = 10;  // 10 ms would be a 100HZ slice

    protected final StatsDClientErrorHandler handler;

    final int maxPacketSizeBytes;
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
        char[] charBuffer = new char[maxPacketSizeBytes];
        // + 4 so that we can check for buffer overflow without computing encoded length first
        final byte[] byteBuffer = new byte[maxPacketSizeBytes + 4];

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

                    boolean partialWrite;
                    do {
                        builder.setLength(0);
                        partialWrite = message.writeTo(builder, sendBuffer.capacity(), containerID);
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
                    }
                    while (partialWrite);

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
            if (length > charBuffer.length) {
                charBuffer = new char[length];
            }

            // We trust this returns valid UTF-16.
            builder.getChars(0, length, charBuffer, 0);

            int blen = 0;
            for (int i = 0; i < length; i++) {
                char ch = charBuffer[i];
                // https://en.wikipedia.org/wiki/UTF-8#Description
                // https://en.wikipedia.org/wiki/UTF-16#Description
                if (ch < 0x80) {
                    byteBuffer[blen++] = (byte)ch;
                } else if (ch < 0x800) {
                    byteBuffer[blen++] = (byte)(192 | (ch >> 6));
                    byteBuffer[blen++] = (byte)(128 | (ch & 63));
                } else if (ch < 0xd800 || ch >= 0xe000) {
                    byteBuffer[blen++] = (byte)(224 | (ch >> 12));
                    byteBuffer[blen++] = (byte)(128 | ((ch >> 6) & 63));
                    byteBuffer[blen++] = (byte)(128 | (ch & 63));
                } else {
                    // surrogate pair
                    int decoded = ((ch & 0x3ff) << 10) | (charBuffer[++i] & 0x3ff) | 0x10000;
                    byteBuffer[blen++] = (byte)(240 | (decoded >> 18));
                    byteBuffer[blen++] = (byte)(128 | ((decoded >> 12) & 63));
                    byteBuffer[blen++] = (byte)(128 | ((decoded >> 6) & 63));
                    byteBuffer[blen++] = (byte)(128 | (decoded & 63));
                }

                if (blen >= maxPacketSizeBytes) {
                    throw new BufferOverflowException();
                }
            }

            sendBuffer.mark();
            sendBuffer.put(byteBuffer, 0, blen);
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

        this.maxPacketSizeBytes = maxPacketSizeBytes;
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
