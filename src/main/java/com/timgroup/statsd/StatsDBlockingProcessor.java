package com.timgroup.statsd;

import com.timgroup.statsd.Message;

import java.nio.ByteBuffer;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class StatsDBlockingProcessor extends StatsDProcessor {

    private final BlockingQueue<Message> messages;

    StatsDBlockingProcessor(final int queueSize, final StatsDClientErrorHandler handler,
            final int maxPacketSizeBytes, final int poolSize, final int workers)
            throws Exception {

        super(queueSize, handler, maxPacketSizeBytes, poolSize, workers);
        this.messages = new ArrayBlockingQueue<String>(queueSize);
    }

    @Override
    boolean send(final Message message);
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
                    StringBuilder builder = new StringBuilder();
                    CharBuffer charBuffer = CharBuffer.wrap(builder);

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

                            final Message message = messages.poll(WAIT_SLEEP_MS, TimeUnit.MILLISECONDS);
                            // TODO: revisit this logic - cleanup, remove duplicate code.
                            if (message != null) {

                                builder.setLength(0);

                                message.writeTo(builder);
                                int lowerBoundSize = builder.length();

                                // if (sendBuffer.capacity() < data.length) {
                                //     throw new InvalidMessageException(MESSAGE_TOO_LONG, message);
                                // }

                                if (sendBuffer.remaining() < (lowerBoundSize + 1)) {
                                    outboundQueue.put(sendBuffer);
                                    sendBuffer = bufferPool.borrow();
                                }

                                sendBuffer.mark();
                                if (sendBuffer.position() > 0) {
                                    sendBuffer.put((byte) '\n');
                                }

                                try {
                                    charBuffer = writeBuilderToSendBuffer(builder, charBuffer, sendBuffer);
                                } catch (BufferOverflowException boe) {
                                    outboundQueue.put(sendBuffer);
                                    sendBuffer = bufferPool.borrow();
                                    charBuffer = writeBuilderToSendBuffer(builder, charBuffer, sendBuffer);
                                }

                                // TODO: revisit this logic
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

                    builder.setLength(0);
                    builder.trimToSize();
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
