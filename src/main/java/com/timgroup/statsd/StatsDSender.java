package com.timgroup.statsd;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class StatsDSender implements Runnable {
    private final Callable<SocketAddress> addressLookup;
    private final SocketAddress address;
    private final DatagramChannel clientChannel;
    private final StatsDClientErrorHandler handler;

    private final BufferPool pool;
    private final BlockingQueue<ByteBuffer> buffers;
    private static final int WAIT_SLEEP_MS = 10;  // 10 ms would be a 100HZ slice

    private final ExecutorService executor;
    private static final int DEFAULT_WORKERS = 1;
    private final int workers;

    private final CountDownLatch endSignal;
    private volatile boolean shutdown;

    private final Telemetry telemetry;


    StatsDSender(final Callable<SocketAddress> addressLookup, final DatagramChannel clientChannel,
                 final StatsDClientErrorHandler handler, BufferPool pool, BlockingQueue<ByteBuffer> buffers,
                 final int workers, final Telemetry telemetry) throws Exception {

        this.pool = pool;
        this.buffers = buffers;
        this.handler = handler;
        this.workers = workers;

        this.addressLookup = addressLookup;
        this.address = addressLookup.call();
        this.clientChannel = clientChannel;

        this.executor = Executors.newFixedThreadPool(workers, new ThreadFactory() {
            final ThreadFactory delegate = Executors.defaultThreadFactory();
            @Override public Thread newThread(final Runnable runnable) {
                final Thread result = delegate.newThread(runnable);
                result.setName("StatsD-Sender-" + result.getName());
                result.setDaemon(true);
                return result;
            }
        });
        this.endSignal = new CountDownLatch(workers);

        this.telemetry = telemetry;
    }

    StatsDSender(final Callable<SocketAddress> addressLookup, final DatagramChannel clientChannel,
                 final StatsDClientErrorHandler handler, BufferPool pool, BlockingQueue<ByteBuffer> buffers,
                 final Telemetry telemetry) throws Exception {
        this(addressLookup, clientChannel, handler, pool, buffers, DEFAULT_WORKERS, telemetry);
    }

    StatsDSender(final StatsDSender sender) throws Exception {
        this(sender.addressLookup, sender.clientChannel, sender.handler,
                sender.pool, sender.buffers, sender.workers, sender.telemetry);
    }

    StatsDSender(final StatsDSender sender, BufferPool pool, BlockingQueue<ByteBuffer> buffers) throws Exception {
        this(sender.addressLookup, sender.clientChannel, sender.handler,
                pool, buffers, sender.workers, sender.telemetry);
    }

    @Override
    public void run() {

        for (int i = 0 ; i < workers ; i++) {
            executor.submit(new Runnable() {
                public void run() {
                    ByteBuffer buffer = null;

                    while (!(buffers.isEmpty() && shutdown)) {
                        int sizeOfBuffer = 0;
                        try {

                            if (buffer != null) {
                                pool.put(buffer);
                            }

                            buffer = buffers.poll(WAIT_SLEEP_MS, TimeUnit.MILLISECONDS);
                            if (buffer == null) {
                                continue;
                            }

                            sizeOfBuffer = buffer.position();

                            buffer.flip();
                            final int sentBytes = clientChannel.send(buffer, address);

                            buffer.clear();
                            if (sizeOfBuffer != sentBytes) {
                                throw new IOException(
                                        String.format("Could not send stat %s entirely to %s. Only sent %d out of %d bytes",
                                            buffer.toString(),
                                            address.toString(),
                                            sentBytes,
                                            sizeOfBuffer));
                            }

                            telemetry.incrBytesSent(sizeOfBuffer);
                            telemetry.incrPacketSent(1);

                        } catch (final InterruptedException e) {
                            if (shutdown) {
                                endSignal.countDown();
                                return;
                            }
                        } catch (final Exception e) {
                            telemetry.incrBytesDropped(sizeOfBuffer);
                            telemetry.incrPacketDropped(1);
                            handler.handle(e);
                        }
                    }
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

    boolean isShutdown() {
        return shutdown;
    }

    void shutdown() {
        shutdown = true;
        executor.shutdown();
    }
}
