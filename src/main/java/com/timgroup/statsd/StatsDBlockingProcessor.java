package com.timgroup.statsd;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.charset.Charset;
import java.util.concurrent.Callable;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class StatsDBlockingProcessor extends StatsDProcessor {

    private final BlockingQueue<String> messages;

    StatsDBlockingProcessor(final int queueSize, final StatsDClientErrorHandler handler,
            final int maxPacketSizeBytes, final int poolSize)
            throws Exception {

        super(queueSize, handler, maxPacketSizeBytes, poolSize);
        this.messages = new ArrayBlockingQueue<String>(queueSize);
    }

    @Override
    boolean send(final String message) {
        try {
            if (!shutdown) {
                messages.put(message);
                return true;
            }
        } catch (InterruptedException e) {
        }

        return false;
    }

    @Override
    public void run() {
        boolean empty;
        ByteBuffer sendBuffer;

        try {
            sendBuffer = bufferPool.borrow();
        } catch(final InterruptedException e) {
            handler.handle(e);
            return;
        }

        while (!(messages.isEmpty() && shutdown)) {

            try {

                if (Thread.interrupted()) {
                    return;
                }

                final String message = messages.poll( WAIT_SLEEP_MS, TimeUnit.MILLISECONDS);
                if (message != null) {
                    final byte[] data = message.getBytes(MESSAGE_CHARSET);
                    if (sendBuffer.capacity() < data.length) {
                        throw new InvalidMessageException(MESSAGE_TOO_LONG, message);
                    }
                    if (sendBuffer.remaining() < (data.length + 1)) {
                        outboundQueue.put(sendBuffer);
                        sendBuffer = bufferPool.borrow();
                    }
                    if (sendBuffer.position() > 0) {
                        sendBuffer.put((byte) '\n');
                    }
                    sendBuffer.put(data);
                    if (null == messages.peek()) {
                        outboundQueue.put(sendBuffer);
                        sendBuffer = bufferPool.borrow();
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

    boolean isShutdown() {
        return shutdown;
    }

    void shutdown() {
        shutdown = true;
    }
}
