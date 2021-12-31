package com.timgroup.statsd;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;


public class StatsDBlockingProcessor extends StatsDProcessor {

    private final BlockingQueue<Message> messages;

    private class ProcessingTask extends StatsDProcessor.ProcessingTask {

        @Override
        protected void processLoop() {
            ByteBuffer sendBuffer;
            boolean empty = true;
            boolean emptyHighPrio = true;

            try {
                sendBuffer = bufferPool.borrow();
            } catch (final InterruptedException e) {
                handler.handle(e);
                return;
            }

            while (!((emptyHighPrio = highPrioMessages.isEmpty()) && messages.isEmpty() && shutdown)) {

                try {

                    final Message message;
                    if (!emptyHighPrio) {
                        message = highPrioMessages.poll();
                    } else {
                        message = messages.poll(WAIT_SLEEP_MS, TimeUnit.MILLISECONDS);
                    }

                    if (message != null) {

                        if (aggregator.aggregateMessage(message)) {
                            continue;
                        }

                        builder.setLength(0);

                        message.writeTo(builder);
                        int lowerBoundSize = builder.length();

                        if (sendBuffer.capacity() < lowerBoundSize) {
                            throw new InvalidMessageException(MESSAGE_TOO_LONG, builder.toString());
                        }

                        if (sendBuffer.remaining() < (lowerBoundSize + 1)) {
                            outboundQueue.put(sendBuffer);
                            sendBuffer = bufferPool.borrow();
                        }

                        sendBuffer.mark();

                        try {
                            writeBuilderToSendBuffer(sendBuffer);
                        } catch (BufferOverflowException boe) {
                            outboundQueue.put(sendBuffer);
                            sendBuffer = bufferPool.borrow();
                            writeBuilderToSendBuffer(sendBuffer);
                        }

                        if (null == messages.peek()) {
                            outboundQueue.put(sendBuffer);
                            sendBuffer = bufferPool.borrow();
                        }
                    }
                } catch (final InterruptedException e) {
                    if (shutdown) {
                        break;
                    }
                } catch (final Exception e) {
                    handler.handle(e);
                }
            }

            builder.setLength(0);
            builder.trimToSize();
        }

    }

    StatsDBlockingProcessor(final int queueSize, final StatsDClientErrorHandler handler,
            final int maxPacketSizeBytes, final int poolSize, final int workers,
            final int aggregatorFlushInterval, final int aggregatorShards,
            final ThreadFactory threadFactory) throws Exception {

        super(queueSize, handler, maxPacketSizeBytes, poolSize, workers,
                aggregatorFlushInterval, aggregatorShards, threadFactory);
        this.messages = new ArrayBlockingQueue<>(queueSize);
    }

    @Override
    protected ProcessingTask createProcessingTask() {
        return new ProcessingTask();
    }

    @Override
    protected boolean send(final Message message) {
        try {
            if (!shutdown) {
                messages.put(message);
                return true;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        return false;
    }
}
