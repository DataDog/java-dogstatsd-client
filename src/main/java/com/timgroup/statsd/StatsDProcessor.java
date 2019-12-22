package com.timgroup.statsd;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.charset.Charset;
import java.util.concurrent.Callable;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class StatsDProcessor implements Runnable {
    private static final Charset MESSAGE_CHARSET = Charset.forName("UTF-8");
    private static final String MESSAGE_TOO_LONG = "Message longer than size of sendBuffer";
    private static final int WAIT_SLEEP_MS = 10;  // 10 ms would be a 100HZ slice

    private final StatsDClientErrorHandler handler;

    private final BufferPool bufferPool;

    private final int qCapacity;
    private final Queue<String> messages;
    private final AtomicInteger qSize;  // qSize will not reflect actual size, but a close estimate.
    private final BlockingQueue<ByteBuffer> outboundQueue; // FIFO queue with outbound buffers

    private volatile boolean shutdown;


    StatsDProcessor(final int queueSize, final StatsDClientErrorHandler handler,
            final int maxPacketSizeBytes, final int poolSize)
            throws Exception {

        this.bufferPool = new BufferPool(poolSize, maxPacketSizeBytes, true);

        this.qSize = new AtomicInteger(0);
        this.qCapacity = queueSize;

        this.messages = new ConcurrentLinkedQueue<String>();
        this.outboundQueue = new ArrayBlockingQueue<ByteBuffer>(poolSize);

        this.handler = handler;
    }

    boolean send(final String message) {
        if (!shutdown) {
            if (qSize.get() < qCapacity) {
                messages.offer(message);
                qSize.incrementAndGet();
                return true;
            }
        }

        return false;
    }

    public BufferPool getBufferPool() {
        return this.bufferPool;
    }

    public BlockingQueue<ByteBuffer> getOutboundQueue() {
        return this.outboundQueue;
    }

    @Override
    public void run() {
        boolean empty;
        ByteBuffer sendBuffer;

        try {
            sendBuffer = bufferPool.borrow();
        } catch(final InterruptedException e) {
            // TODO
            return;
        }

        while (!((empty = messages.isEmpty()) && shutdown)) {

            try {
                if (empty) {
                    Thread.sleep(WAIT_SLEEP_MS);
                    continue;
                }

                if (Thread.interrupted()) {
                    return;
                }
                final String message = messages.poll();
                if (message != null) {
                    qSize.decrementAndGet();
                    final byte[] data = message.getBytes(MESSAGE_CHARSET);
                    if (sendBuffer.capacity() < data.length) {
                        throw new InvalidMessageException(MESSAGE_TOO_LONG, message);
                    }
                    if (sendBuffer.remaining() < (data.length + 1)) {
                        outboundQueue.put(sendBuffer);
                        sendBuffer = bufferPool.borrow();
                    }
                    if (sendBuffer.position() > 0) {
                        sendBuffer.put((byte) '\n');
                    }
                    sendBuffer.put(data);
                    if (null == messages.peek()) {
                        outboundQueue.put(sendBuffer);
                        sendBuffer = bufferPool.borrow();
                    }
                }
            } catch (final InterruptedException e) {
                if (shutdown) {
                    return;
                }
            } catch (final Exception e) {
                handler.handle(e);
            }
        }
    }

    boolean isShutdown() {
        return shutdown;
    }

    void shutdown() {
        shutdown = true;
    }
}
