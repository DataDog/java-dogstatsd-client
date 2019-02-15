package com.timgroup.statsd;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.charset.Charset;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class StatsDSender implements Runnable {
    private static final Charset MESSAGE_CHARSET = Charset.forName("UTF-8");
    private static final int PACKET_SIZE_BYTES = 1400;


    private final ByteBuffer sendBuffer = ByteBuffer.allocate(PACKET_SIZE_BYTES);
    private final Callable<SocketAddress> addressLookup;
    private final BlockingQueue<String> queue;
    private final StatsDClientErrorHandler handler;
    private final DatagramChannel clientChannel;

    private volatile boolean shutdown;


    StatsDSender(final Callable<SocketAddress> addressLookup, final int queueSize,
                 final StatsDClientErrorHandler handler, final DatagramChannel clientChannel) {
        this(addressLookup,  new LinkedBlockingQueue<String>(queueSize), handler, clientChannel);
    }

    StatsDSender(final Callable<SocketAddress> addressLookup, final BlockingQueue<String> queue,
                 final StatsDClientErrorHandler handler, final DatagramChannel clientChannel) {
        this.addressLookup = addressLookup;
        this.queue = queue;
        this.handler = handler;
        this.clientChannel = clientChannel;
    }


    boolean send(final String message) {
        if (!shutdown) {
            queue.offer(message);
            return true;
        }
        return false;
    }

    @Override
    public void run() {
        while (!(queue.isEmpty() && shutdown)) {
            try {
                if (Thread.interrupted()) {
                    return;
                }
                final String message = queue.poll(1, TimeUnit.SECONDS);
                if (null != message) {
                    final byte[] data = message.getBytes(MESSAGE_CHARSET);
                    if (sendBuffer.capacity() < data.length) {
                        throw new UnsendableMessageException(message);
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