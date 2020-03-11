package com.timgroup.statsd;

import com.timgroup.statsd.Message;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class StatsDProcessor implements Runnable {
    protected static final Charset MESSAGE_CHARSET = Charset.forName("UTF-8");

    protected static final String MESSAGE_TOO_LONG = "Message longer than size of sendBuffer";
    protected static final int WAIT_SLEEP_MS = 10;  // 10 ms would be a 100HZ slice

    protected final StatsDClientErrorHandler handler;

    protected final BufferPool bufferPool;
    protected final List<StringBuilder> builders; // StringBuilders for processing, 1 per worker
    protected final List<CharBuffer> charBuffers; // CharBuffers for processing, 1 per worker
    protected final BlockingQueue<ByteBuffer> outboundQueue; // FIFO queue with outbound buffers
    protected final ExecutorService executor;
    protected final CountDownLatch endSignal;

    protected final int workers;

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
            final int maxPacketSizeBytes, final int poolSize, final int workers)
            throws Exception {

        this.handler = handler;
        this.workers = workers;

        this.executor = Executors.newFixedThreadPool(workers);
        this.bufferPool = new BufferPool(poolSize, maxPacketSizeBytes, true);
        this.outboundQueue = new ArrayBlockingQueue<ByteBuffer>(poolSize);
        this.endSignal = new CountDownLatch(workers);

        this.builders = new ArrayList<>(workers);
        this.charBuffers = new ArrayList<>(workers);
        for(int i=0; i<workers ; i++) {
            StringBuilder builder = new StringBuilder();
            CharBuffer buffer = CharBuffer.wrap(builder);
            builders.add(builder);
            charBuffers.add(buffer);
        }
    }

    protected abstract ProcessingTask createProcessingTask();

    protected abstract boolean send(final Message message);

    public BufferPool getBufferPool() {
        return this.bufferPool;
    }

    public BlockingQueue<ByteBuffer> getOutboundQueue() {
        return this.outboundQueue;
    }

    @Override
    public abstract void run();

    boolean isShutdown() {
        return shutdown;
    }

    void shutdown() {
        shutdown = true;
    }
}
