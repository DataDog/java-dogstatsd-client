package com.timgroup.statsd;

import com.timgroup.statsd.Message;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;


public class StatsDBlockingProcessor extends StatsDProcessor {

    private final BlockingQueue<Message>[] messages;
    private final BlockingQueue<Integer>[] processorWorkQueue;

    private class ProcessingTask extends StatsDProcessor.ProcessingTask {

        public ProcessingTask(int id) {
            super(id);
        }

        @Override
        public void run() {
            ByteBuffer sendBuffer;
            boolean empty = true;
            boolean emptyHighPrio = true;

            try {
                sendBuffer = bufferPool.borrow();
            } catch (final InterruptedException e) {
                handler.handle(e);
                return;
            }

            aggregator.start();

            while (!((emptyHighPrio = highPrioMessages.isEmpty()) && processorWorkQueue[this.processorQueueId].isEmpty() && shutdown)) {

                try {

                    final Message message;
                    if (!emptyHighPrio) {
                        message = highPrioMessages.poll();
                    } else {
                        final int messageQueueIdx = processorWorkQueue[this.processorQueueId].poll();
                        message = messages[messageQueueIdx].poll(WAIT_SLEEP_MS, TimeUnit.MILLISECONDS);
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

                        if (null == processorWorkQueue[this.processorQueueId].peek()) {
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
            aggregator.stop();
            endSignal.countDown();
        }
    }

    StatsDBlockingProcessor(final int queueSize, final StatsDClientErrorHandler handler,
            final int maxPacketSizeBytes, final int poolSize, final int workers, final int lockShardGrain,
            final int aggregatorFlushInterval, final int aggregatorShards) throws Exception {

        super(queueSize, handler, maxPacketSizeBytes, poolSize, workers, lockShardGrain, aggregatorFlushInterval, aggregatorShards);

        this.messages = new ArrayBlockingQueue[lockShardGrain];
        for (int i = 0 ; i < lockShardGrain ; i++) {
            this.messages[i] = new ArrayBlockingQueue<Message>(queueSize);
        }

        this.processorWorkQueue = new ArrayBlockingQueue[workers];
        for (int i = 0 ; i < workers ; i++) {
            this.processorWorkQueue[i] = new ArrayBlockingQueue<Integer>(queueSize);
        }
    }

    @Override
    protected ProcessingTask createProcessingTask(int id) {
        return new ProcessingTask(id);
    }

    StatsDBlockingProcessor(final StatsDBlockingProcessor processor)
            throws Exception {

        super(processor);

        this.messages = new ArrayBlockingQueue[lockShardGrain];
        for (int i = 0 ; i < lockShardGrain ; i++) {
            this.messages[i] = new ArrayBlockingQueue<Message>(getQcapacity());
        }

        this.processorWorkQueue = new ArrayBlockingQueue[workers];
        for (int i = 0 ; i < workers ; i++) {
            this.processorWorkQueue[i] = new ArrayBlockingQueue<Integer>(getQcapacity());
        }
    }

    @Override
    protected boolean send(final Message message) {
        try {
            int threadId = getThreadId();
            int shard = threadId % lockShardGrain;
            int processQueue = threadId % workers;

            if (!shutdown) {
                messages[shard].put(message);
                processorWorkQueue[processQueue].put(shard);
                return true;
            }
        } catch (InterruptedException e) {
            // NOTHING
        }

        return false;
    }
}
