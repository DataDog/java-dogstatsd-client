package com.timgroup.statsd;

import com.timgroup.statsd.Message;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class StatsDNonBlockingProcessor extends StatsDProcessor {

    private final Queue<Message> messages;
    private final int qcapacity;
    private final AtomicInteger qsize;  // qSize will not reflect actual size, but a close estimate.

    private class ProcessingTask implements Runnable {
        private final int id;

        ProcessingTask(int id) {
            this.id = id;
        }

        @Override
        public void run() {
            boolean empty;
            ByteBuffer sendBuffer;
            StringBuilder builder = builders.get(id);

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
                    final Message message = messages.poll();
                    // TODO: revisit this logic - cleanup, remove duplicate code.
                    if (message != null) {

                        qsize.decrementAndGet();
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
                            writeBuilderToSendBuffer(id, builder, sendBuffer);
                        } catch (BufferOverflowException boe) {
                            outboundQueue.put(sendBuffer);
                            sendBuffer = bufferPool.borrow();
                            writeBuilderToSendBuffer(id, builder, sendBuffer);
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
    }

    StatsDNonBlockingProcessor(final int queueSize, final StatsDClientErrorHandler handler,
            final int maxPacketSizeBytes, final int poolSize, final int workers)
            throws Exception {

        super(queueSize, handler, maxPacketSizeBytes, poolSize, workers);
        this.qsize = new AtomicInteger(0);
        this.qcapacity = queueSize;
        this.messages = new ConcurrentLinkedQueue<>();
    }

    @Override
    boolean send(final Message message){
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
            executor.submit(new ProcessingTask(i));
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
