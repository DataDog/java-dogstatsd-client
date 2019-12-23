package com.timgroup.statsd;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.charset.Charset;
import java.util.concurrent.Callable;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.TimeUnit;

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


    StatsDSender(final Callable<SocketAddress> addressLookup, final DatagramChannel clientChannel,
                 final StatsDClientErrorHandler handler, BufferPool pool, BlockingQueue<ByteBuffer> buffers,
                 final int workers) throws Exception {

        this.pool = pool;
        this.buffers = buffers;
        this.handler = handler;
        this.workers = workers;

        this.addressLookup = addressLookup;
        this.address = addressLookup.call();
        this.clientChannel = clientChannel;

        this.executor = Executors.newFixedThreadPool(workers);
        this.endSignal = new CountDownLatch(workers);
    }

    StatsDSender(final Callable<SocketAddress> addressLookup, final DatagramChannel clientChannel,
                 final StatsDClientErrorHandler handler, BufferPool pool, BlockingQueue<ByteBuffer> buffers) throws Exception {
        this(addressLookup, clientChannel, handler, pool, buffers, DEFAULT_WORKERS);
    }

    @Override
    public void run() {

        for (int i=0 ; i<workers ; i++) {
            executor.submit(new Runnable() {
                public void run() {
                    ByteBuffer buffer = null;

                    while (!(buffers.isEmpty() && shutdown)) {
                        try {

                            if (buffer != null) {
                                pool.put(buffer);
                            }

                            buffer = buffers.poll(WAIT_SLEEP_MS, TimeUnit.MILLISECONDS);
                            if (buffer == null) {
                                continue;
                            }

                            final int sizeOfBuffer = buffer.position();

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

                        } catch (final InterruptedException e) {
                            if (shutdown) {
                                endSignal.countDown();
                                return;
                            }
                        } catch (final Exception e) {
                            handler.handle(e);
                        }
                    }
                    endSignal.countDown();
                }
            });
        }

        boolean done = false;
        while(!done) {
            try {
                endSignal.await();
                done = true;
            } catch (final InterruptedException e) { }
        }
    }

    boolean isShutdown() {
        return shutdown;
    }

    void shutdown() {
        shutdown = true;
    }
}
