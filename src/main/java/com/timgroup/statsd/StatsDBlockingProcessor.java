package com.timgroup.statsd;

import com.timgroup.statsd.Message;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;


public class StatsDBlockingProcessor extends StatsDProcessor {

    private final BlockingQueue<Message> messages;
    private final StatsDAggregator aggregator;

    private class ProcessingTask extends StatsDProcessor.ProcessingTask {

        @Override
        public void run() {
            boolean empty;
            ByteBuffer sendBuffer;

            try {
                sendBuffer = bufferPool.borrow();
            } catch (final InterruptedException e) {
                handler.handle(e);
                return;
            }

            while (!(shutdown && messages.isEmpty())) {

                try {

                    final Message message = messages.poll(WAIT_SLEEP_MS, TimeUnit.MILLISECONDS);
                    if (message != null) {

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
                        if (sendBuffer.position() > 0) {
                            sendBuffer.put((byte) '\n');
                        }

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

    }

    StatsDBlockingProcessor(final int queueSize, final StatsDClientErrorHandler handler,
            final int maxPacketSizeBytes, final int poolSize, final int workers)
            throws Exception {

        super(queueSize, handler, maxPacketSizeBytes, poolSize, workers);
        this.messages = new ArrayBlockingQueue<>(queueSize);
        this.aggregator = new StatsDAggregator(this, 15);  // TODO: fix period
    }

    @Override
    protected ProcessingTask createProcessingTask() {
        return new ProcessingTask();
    }

    StatsDBlockingProcessor(final StatsDBlockingProcessor processor)
            throws Exception {

        super(processor);
        this.messages = new ArrayBlockingQueue<>(processor.getQcapacity());
        this.aggregator = new StatsDAggregator(this, 15);  // TODO: fix period
    }

    @Override
    protected boolean send(final Message message) {
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
}
