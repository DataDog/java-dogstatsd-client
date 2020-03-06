package com.timgroup.statsd;

import java.nio.ByteBuffer;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class StatsDNonBlockingProcessor extends StatsDProcessor {

    private final Queue<String> messages;
    private final int qcapacity;
    private final AtomicInteger qsize;  // qSize will not reflect actual size, but a close estimate.

    StatsDNonBlockingProcessor(final int queueSize, final StatsDClientErrorHandler handler,
            final int maxPacketSizeBytes, final int poolSize, final int workers)
            throws Exception {

        super(queueSize, handler, maxPacketSizeBytes, poolSize, workers);
        this.qsize = new AtomicInteger(0);
        this.qcapacity = queueSize;
        this.messages = new ConcurrentLinkedQueue<String>();
    }

    @Override
    boolean send(final String message) {
        if (!shutdown) {
            if (qsize.get() < qcapacity) {
                messages.offer(message);
                qsize.incrementAndGet();
                return true;
            }
        }

        return false;
    }

    @Override
    public void run() {

        for (int i = 0 ; i < workers ; i++) {
            executor.submit(new Runnable() {
                public void run() {
                    boolean empty;
                    ByteBuffer sendBuffer;

                    try {
                        sendBuffer = bufferPool.borrow();
                    } catch (final InterruptedException e) {
                        handler.handle(e);
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
                                qsize.decrementAndGet();
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
                                endSignal.countDown();
                                return;
                            }
                        } catch (final Exception e) {
                            handler.handle(e);
                        }
                    }
                    endSignal.countDown();
                }
            });
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

    boolean isShutdown() {
        return shutdown;
    }

    void shutdown() {
        shutdown = true;
    }
}
