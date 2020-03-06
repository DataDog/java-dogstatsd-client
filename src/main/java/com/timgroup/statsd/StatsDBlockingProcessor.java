package com.timgroup.statsd;

import java.nio.ByteBuffer;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class StatsDBlockingProcessor extends StatsDProcessor {

    private final BlockingQueue<String> messages;

    StatsDBlockingProcessor(final int queueSize, final StatsDClientErrorHandler handler,
            final int maxPacketSizeBytes, final int poolSize, final int workers)
            throws Exception {

        super(queueSize, handler, maxPacketSizeBytes, poolSize, workers);
        this.messages = new ArrayBlockingQueue<String>(queueSize);
    }

    @Override
    boolean send(final String message) {
        try {
            if (!shutdown) {
                messages.put(message);
                return true;
            }
        } catch (InterruptedException e) {
            // NOTHING
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

                    while (!(messages.isEmpty() && shutdown)) {

                        try {

                            if (Thread.interrupted()) {
                                return;
                            }

                            final String message = messages.poll(WAIT_SLEEP_MS, TimeUnit.MILLISECONDS);
                            if (message != null) {
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
}
