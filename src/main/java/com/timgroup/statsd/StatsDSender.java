package com.timgroup.statsd;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class StatsDSender implements Runnable {
    private static final Charset MESSAGE_CHARSET = Charset.forName("UTF-8");

    private final CharsetEncoder utf8Encoder = MESSAGE_CHARSET.newEncoder()
            .onMalformedInput(CodingErrorAction.REPLACE)
            .onUnmappableCharacter(CodingErrorAction.REPLACE);

    private final StringBuilder builder = new StringBuilder();
    private CharBuffer charBuffer = CharBuffer.wrap(builder);

    private final ByteBuffer sendBuffer;
    private final Callable<SocketAddress> addressLookup;
    private final BlockingQueue<Message> queue;
    private final StatsDClientErrorHandler handler;
    private final DatagramChannel clientChannel;

    private volatile boolean shutdown;


    StatsDSender(final Callable<SocketAddress> addressLookup, final int queueSize,
                 final StatsDClientErrorHandler handler, final DatagramChannel clientChannel, final int maxPacketSizeBytes) {
        this(addressLookup,  new LinkedBlockingQueue<Message>(queueSize), handler, clientChannel, maxPacketSizeBytes);
    }

    StatsDSender(final Callable<SocketAddress> addressLookup, final BlockingQueue<Message> queue,
                 final StatsDClientErrorHandler handler, final DatagramChannel clientChannel, final int maxPacketSizeBytes) {
        sendBuffer = ByteBuffer.allocate(maxPacketSizeBytes);
        this.addressLookup = addressLookup;
        this.queue = queue;
        this.handler = handler;
        this.clientChannel = clientChannel;
    }

    interface Message {
        /**
         * Write this message to the provided {@link StringBuilder}. Will
         * only ever be called from the sender thread.
         * 
         * @param builder
         */
        void writeTo(StringBuilder builder);
    }

    boolean send(final Message message) {
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
                final Message message = queue.poll(1, TimeUnit.SECONDS);
                if (null != message) {
                    final SocketAddress address = addressLookup.call();
                    builder.setLength(0);
                    message.writeTo(builder);
                    int lowerBoundSize = builder.length();
                    if (sendBuffer.remaining() < (lowerBoundSize + 1)) {
                        blockingSend(address);
                    }
                    sendBuffer.mark();
                    if (sendBuffer.position() > 0) {
                        sendBuffer.put((byte) '\n');
                    }
                    try {
                        writeBuilderToSendBuffer();
                    } catch (BufferOverflowException boe) {
                        sendBuffer.reset();
                        blockingSend(address);
                        writeBuilderToSendBuffer();
                    }
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
        builder.setLength(0);
        builder.trimToSize();
    }

    private void writeBuilderToSendBuffer() {
        int length = builder.length();
        // use existing charbuffer if possible, otherwise re-wrap
        if (length <= charBuffer.capacity()) {
            charBuffer.limit(length).position(0);
        } else {
            charBuffer = CharBuffer.wrap(builder);
        }
        if (utf8Encoder.encode(charBuffer, sendBuffer, true) == CoderResult.OVERFLOW) {
            throw new BufferOverflowException();
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