package com.timgroup.statsd;

import com.timgroup.statsd.Message;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;


public class StatsDNonBlockingProcessor extends StatsDProcessor {

    private final Queue<Message>[] messages;
    private final Queue<Integer>[] processorWorkQueue;

    private final int qcapacity;
    private final AtomicInteger[] qsize;  // qSize will not reflect actual size, but a close estimate.

    private class ProcessingTask extends StatsDProcessor.ProcessingTask {

        public ProcessingTask(int id) {
            super(id);
        }

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

            while (!((empty = processorWorkQueue[this.processorQueueId].isEmpty()) && shutdown)) {

                try {
                    if (empty) {
                        Thread.sleep(WAIT_SLEEP_MS);
                        continue;
                    }

                    if (Thread.interrupted()) {
                        return;
                    }

                    final int messageQueueIdx = processorWorkQueue[this.processorQueueId].poll();
                    final Message message = messages[messageQueueIdx].poll();
                    if (message != null) {
                        qsize[messageQueueIdx].decrementAndGet();
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
            endSignal.countDown();
        }
    }

    StatsDNonBlockingProcessor(final int queueSize, final StatsDClientErrorHandler handler,
            final int maxPacketSizeBytes, final int poolSize, final int workers,
            final int lockShardGrain) throws Exception {

        super(queueSize, handler, maxPacketSizeBytes, poolSize, workers, lockShardGrain);
        this.qsize = new AtomicInteger[lockShardGrain];
        this.qcapacity = queueSize;
        this.messages = new ConcurrentLinkedQueue[lockShardGrain];
        for (int i = 0 ; i < lockShardGrain ; i++) {
            this.qsize[i] = new AtomicInteger();
            this.messages[i] = new ConcurrentLinkedQueue<Message>();
            this.qsize[i].set(0);
        }

        this.processorWorkQueue = new ConcurrentLinkedQueue[workers];
        for (int i = 0 ; i < workers ; i++) {
            this.processorWorkQueue[i] = new ConcurrentLinkedQueue<Integer>();
        }
    }

    @Override
    protected ProcessingTask createProcessingTask(int id) {
        return new ProcessingTask(id);
    }

    @Override
    protected boolean send(final Message message) {
        if (!shutdown) {
            int threadId = getThreadId();
            int shard = threadId % lockShardGrain;
            int processQueue = threadId % workers;

            if (qsize[shard].get() < qcapacity) {
                messages[shard].offer(message);
                qsize[shard].incrementAndGet();
                processorWorkQueue[processQueue].offer(shard);
                return true;
            }
        }

        return false;
    }

    boolean isShutdown() {
        return shutdown;
    }

    void shutdown() {
        shutdown = true;
    }
}
