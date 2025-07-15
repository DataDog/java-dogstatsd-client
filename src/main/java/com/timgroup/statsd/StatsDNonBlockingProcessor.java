package com.timgroup.statsd;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class StatsDNonBlockingProcessor extends StatsDProcessor {

    private final Queue<Message> messages;
    private final AtomicInteger qsize; // qSize will not reflect actual size, but a close estimate.

    private class ProcessingTask extends StatsDProcessor.ProcessingTask {
        @Override
        protected Message getMessage() throws InterruptedException {
            final Message message = messages.poll();
            if (message != null) {
                qsize.decrementAndGet();
                return message;
            }

            Thread.sleep(WAIT_SLEEP_MS);

            return null;
        }

        @Override
        protected boolean haveMessages() {
            return !messages.isEmpty();
        }
    }

    StatsDNonBlockingProcessor(
            final int queueSize,
            final StatsDClientErrorHandler handler,
            final int maxPacketSizeBytes,
            final int poolSize,
            final int workers,
            final int aggregatorFlushInterval,
            final int aggregatorShards,
            final ThreadFactory threadFactory)
            throws Exception {

        super(
                queueSize,
                handler,
                maxPacketSizeBytes,
                poolSize,
                workers,
                aggregatorFlushInterval,
                aggregatorShards,
                threadFactory);
        this.qsize = new AtomicInteger(0);
        this.messages = new ConcurrentLinkedQueue<>();
    }

    @Override
    protected ProcessingTask createProcessingTask() {
        return new ProcessingTask();
    }

    @Override
    protected boolean send(final Message message) {
        if (!shutdown) {
            if (qsize.get() < qcapacity) {
                messages.offer(message);
                qsize.incrementAndGet();
                return true;
            }
        }

        return false;
    }
}
