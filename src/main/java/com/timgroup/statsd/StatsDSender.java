package com.timgroup.statsd;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class StatsDSender {
    private final WritableByteChannel clientChannel;
    private final StatsDClientErrorHandler handler;

    private final BufferPool pool;
    private final BlockingQueue<ByteBuffer> buffers;
    private static final int WAIT_SLEEP_MS = 10; // 10 ms would be a 100HZ slice

    protected final ThreadFactory threadFactory;
    protected final Thread[] workers;

    private final CountDownLatch endSignal;
    private volatile boolean shutdown;

    private volatile Telemetry telemetry;

    StatsDSender(
            final WritableByteChannel clientChannel,
            final StatsDClientErrorHandler handler,
            BufferPool pool,
            BlockingQueue<ByteBuffer> buffers,
            final int workers,
            final ThreadFactory threadFactory) {

        this.pool = pool;
        this.buffers = buffers;
        this.handler = handler;
        this.threadFactory = threadFactory;
        this.workers = new Thread[workers];

        this.clientChannel = clientChannel;

        this.endSignal = new CountDownLatch(workers);
    }

    public void setTelemetry(final Telemetry telemetry) {
        this.telemetry = telemetry;
    }

    public Telemetry getTelemetry() {
        return telemetry;
    }

    void startWorkers(final String namePrefix) {
        // each task is a busy loop taking up one thread, so keep it simple and use an array of
        // threads
        for (int i = 0; i < workers.length; i++) {
            workers[i] =
                    threadFactory.newThread(
                            new Runnable() {
                                public void run() {
                                    try {
                                        sendLoop();
                                    } finally {
                                        endSignal.countDown();
                                    }
                                }
                            });
            workers[i].setName(namePrefix + (i + 1));
            workers[i].start();
        }
    }

    void sendLoop() {
        ByteBuffer buffer = null;
        Telemetry telemetry = getTelemetry(); // attribute snapshot to harness CPU cache

        while (!(buffers.isEmpty() && shutdown)) {
            int sizeOfBuffer = 0;
            try {

                if (buffer != null) {
                    buffer.clear();
                    pool.put(buffer);
                }

                buffer = buffers.poll(WAIT_SLEEP_MS, TimeUnit.MILLISECONDS);
                if (buffer == null) {
                    continue;
                }

                sizeOfBuffer = buffer.position();

                buffer.flip();
                final int sentBytes = clientChannel.write(buffer);

                if (sizeOfBuffer != sentBytes) {
                    throw new IOException(
                            String.format(
                                    "Could not send stat %s entirely to %s. Only sent %d out of %d bytes",
                                    buffer, clientChannel, sentBytes, sizeOfBuffer));
                }

                if (telemetry != null) {
                    telemetry.incrBytesSent(sizeOfBuffer);
                    telemetry.incrPacketSent(1);
                }

            } catch (final InterruptedException e) {
                if (shutdown) {
                    break;
                }
            } catch (final Exception e) {
                if (telemetry != null) {
                    telemetry.incrBytesDropped(sizeOfBuffer);
                    telemetry.incrPacketDropped(1);
                }
                handler.handle(e);
            }
        }
    }

    void shutdown(boolean blocking) throws InterruptedException {
        shutdown = true;
        if (blocking) {
            endSignal.await();
        } else {
            for (int i = 0; i < workers.length; i++) {
                workers[i].interrupt();
            }
        }
    }
}
