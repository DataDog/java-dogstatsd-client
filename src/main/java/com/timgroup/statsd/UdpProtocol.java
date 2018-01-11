package com.timgroup.statsd;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.concurrent.Callable;

/**
 * The UDP implementation of the {@link Protocol} interface.
 *
 * @author Pascal Gélinas
 */
final class UdpProtocol implements Protocol {

    private final ByteBuffer sendBuffer = ByteBuffer.allocate(
        Protocol.PACKET_SIZE_BYTES);
    private final Callable<InetSocketAddress> addressLookup;
    private final DatagramChannel clientChannel;
    private final StatsDClientErrorHandler handler;

    UdpProtocol(Callable<InetSocketAddress> addressLookup,
        StatsDClientErrorHandler errorHandler) throws IOException {
        this.addressLookup = addressLookup;
        if (errorHandler == null) {
            handler = DefaultStatsDClient.NO_OP_HANDLER;
        } else {
            handler = errorHandler;
        }
        clientChannel = DatagramChannel.open();
    }

    @Override
    public void close() throws IOException {
        clientChannel.close();
    }

    /**
     * Add the specified StatsD-formatted String to the IO Buffer, if there are still available
     * space. If the message cannot fit in the buffer, the buffer is sent to the server via {@link
     * #flush()} and then the message gets added to the buffer.
     *
     * @param message The StatsD-formatted String.
     * @throws Exception if the addressLookup fails during the send.
     */
    @Override
    public void send(String message) throws IOException {
        final byte[] data = message.getBytes(Protocol.MESSAGE_CHARSET);
        if (sendBuffer.remaining() < (data.length + 1)) {
            flush();
        }
        if (sendBuffer.position() > 0) {
            sendBuffer.put((byte) '\n');
        }
        sendBuffer.put(data);
    }

    /**
     * Send the IO Buffer to the server and ready the buffer to receive more data.
     *
     * @throws Exception if the addressLookup fails.
     */
    @Override
    public void flush() throws IOException {
        final InetSocketAddress address;
        try {
            address = addressLookup.call();
        } catch (Exception e) {
            throw new IOException("Unable to resolve address", e);
        }
        final int sizeOfBuffer = sendBuffer.position();
        sendBuffer.flip();

        final int sentBytes = clientChannel.send(sendBuffer, address);
        sendBuffer.limit(sendBuffer.capacity());
        sendBuffer.rewind();

        if (sizeOfBuffer != sentBytes) {
            handler.handle(
                new IOException(
                    String.format(
                        "Could not send entirely stat %s to host %s:%d. Only sent %d bytes "
                            + "out of %d bytes",
                        sendBuffer.toString(),
                        address.getHostName(),
                        address.getPort(),
                        sentBytes,
                        sizeOfBuffer)));
        }
    }
}
