package com.timgroup.statsd;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;

public class StatsDNonBlockingProcessor extends StatsDProcessor {

    private final Queue<Message> messages;

    private class ProcessingTask extends StatsDProcessor.ProcessingTask {
        @Override
        protected Message getMessage() throws InterruptedException {
            final Message message = messages.poll();
            if (message != null) {
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
        if (qcapacity <= 8192) {
            this.messages = new ArrayBlockingQueue<>(qcapacity);
        } else {
            this.messages = new LinkedBlockingQueue<>(qcapacity);
        }
    }

    @Override
    protected ProcessingTask createProcessingTask() {
        return new ProcessingTask();
    }

    @Override
    protected boolean send(final Message message) {
        if (!shutdown) {
            return messages.offer(message);
        }

        return false;
    }
}
