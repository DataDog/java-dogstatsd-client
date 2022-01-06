package com.timgroup.statsd;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;


public class StatsDBlockingProcessor extends StatsDProcessor {

    private final BlockingQueue<Message> messages;

    private class ProcessingTask extends StatsDProcessor.ProcessingTask {

        @Override
        protected Message getMessage() throws InterruptedException {
            return messages.poll(WAIT_SLEEP_MS, TimeUnit.MILLISECONDS);
        }

        @Override
        protected boolean haveMessages() {
            return !messages.isEmpty();
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
