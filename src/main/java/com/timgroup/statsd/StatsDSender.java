package com.timgroup.statsd;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.charset.Charset;
import java.util.concurrent.Callable;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.TimeUnit;

public class StatsDSender implements Runnable {
    private static final Charset MESSAGE_CHARSET = Charset.forName("UTF-8");
    private static final String MESSAGE_TOO_LONG = "Message longer than size of sendBuffer";
    private static final int WAIT_SLEEP_MS = 100;

    private final ByteBuffer sendBuffer;
    private final Callable<SocketAddress> addressLookup;
    private final StatsDClientErrorHandler handler;
    private final DatagramChannel clientChannel;

    private final int qCapacity;
    private final Queue<String> queue;
    private final AtomicInteger qSize;  // qSize will not reflect actual size, but a close estimate.

    private volatile boolean shutdown;


    StatsDSender(final Callable<SocketAddress> addressLookup, final int queueSize,
                 final StatsDClientErrorHandler handler, final DatagramChannel clientChannel, final int maxPacketSizeBytes) {
        this.qSize = new AtomicInteger(0);
        this.qCapacity = queueSize;
        this.addressLookup = addressLookup;
        this.queue = new ConcurrentLinkedQueue<String>();
        this.handler = handler;
        this.clientChannel = clientChannel;
        sendBuffer = ByteBuffer.allocateDirect(maxPacketSizeBytes);
    }

    boolean send(final String message) {
        if (!shutdown) {
            if (qSize.get() < qCapacity) {
                queue.offer(message);
                qSize.incrementAndGet();
                return true;
            }
        }

        return false;
    }

    @Override
    public void run() {
        boolean empty;
        while (!((empty = queue.isEmpty()) && shutdown)) {
            try {
                if (empty) {
                    Thread.sleep(WAIT_SLEEP_MS);
                    continue;
                }

                if (Thread.interrupted()) {
                    return;
                }
                final String message = queue.poll();
                if (message != null) {
                    final byte[] data = message.getBytes(MESSAGE_CHARSET);
                    if (sendBuffer.capacity() < data.length) {
                        throw new InvalidMessageException(MESSAGE_TOO_LONG, message);
                    }
                    final SocketAddress address = addressLookup.call();
                    if (sendBuffer.remaining() < (data.length + 1)) {
                        blockingSend(address);
                    }
                    if (sendBuffer.position() > 0) {
                        sendBuffer.put((byte) '\n');
                    }
                    sendBuffer.put(data);
                    if (null == queue.peek()) {
                        blockingSend(address);
                    }
                }
            } catch (final InterruptedException e) {
                if (shutdown) {
                    return;
                }
            } catch (final Exception e) {
                handler.handle(e);
            }
        }
    }

    private void blockingSend(final SocketAddress address) throws IOException {
        final int sizeOfBuffer = sendBuffer.position();
        sendBuffer.flip();

        final int sentBytes = clientChannel.send(sendBuffer, address);
        sendBuffer.limit(sendBuffer.capacity());
        sendBuffer.rewind();

        if (sizeOfBuffer != sentBytes) {
            handler.handle(
                    new IOException(
                            String.format(
                                    "Could not send entirely stat %s to %s. Only sent %d bytes out of %d bytes",
                                    sendBuffer.toString(),
                                    address.toString(),
                                    sentBytes,
                                    sizeOfBuffer)));
        }
    }

    boolean isShutdown() {
        return shutdown;
    }

    void shutdown() {
        shutdown = true;
    }
}
