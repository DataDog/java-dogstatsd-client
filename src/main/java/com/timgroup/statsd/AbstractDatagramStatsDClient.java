package com.timgroup.statsd;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.concurrent.Callable;

abstract class AbstractDatagramStatsDClient extends AbstractStatsDClient {

    private final DatagramChannel clientChannel;
    private final Callable<InetSocketAddress> addressLookup;

    /**
     * @param prefix
     *     the prefix to apply to keys sent via this client
     * @param constantTags
     *     tags to be added to all content sent
     * @param errorHandler
     *     handler to use when an exception occurs during usage, may be null to indicate noop
     * @param addressLookup
     *     yields the IP address and socket of the StatsD server
     * @throws StatsDClientException
     *     if the client could not be started
     */
    AbstractDatagramStatsDClient(String prefix, String[] constantTags, StatsDClientErrorHandler errorHandler, Callable<InetSocketAddress> addressLookup) throws StatsDClientException {
        super(prefix, constantTags);

        this.addressLookup = addressLookup;
        try {
            clientChannel = DatagramChannel.open();
        } catch (final Exception e) {
            throw new StatsDClientException("Failed to start StatsD client", e);
        }
    }

    @Override
    public void stop() {
        try {
            clientChannel.close();
        } catch (IOException e) {
            throw new StatsDClientException("Unable to close client", e);
        }
    }

    void sendBlocking(ByteBuffer sendBuffer) throws Exception {
        int sizeOfBuffer = sendBuffer.limit();
        InetSocketAddress address = addressLookup.call();
        int sentBytes = clientChannel.send(sendBuffer, address);
        if (sizeOfBuffer != sentBytes) {
            throw new IOException(
                    String.format(
                            "Could not send entirely stat %s to host %s. Only sent %d bytes out of %d bytes",
                            sendBuffer.toString(),
                            address.toString(),
                            sentBytes,
                            sizeOfBuffer));
        }
    }

    /**
     * Create dynamic lookup for the given host name and port.
     *
     * @param hostname the host name of the targeted StatsD server
     * @param port     the port of the targeted StatsD server
     * @return a function to perform the lookup
     */
    public static Callable<InetSocketAddress> volatileAddressResolution(final String hostname, final int port) {
        return new Callable<InetSocketAddress>() {
            @Override
            public InetSocketAddress call() throws UnknownHostException {
                return new InetSocketAddress(InetAddress.getByName(hostname), port);
            }
        };
    }

    /**
     * Lookup the address for the given host name and cache the result.
     *
     * @param hostname the host name of the targeted StatsD server
     * @param port     the port of the targeted StatsD server
     * @return a function that cached the result of the lookup
     * @throws Exception if the lookup fails, i.e. {@link UnknownHostException}
     */
    public static Callable<InetSocketAddress> staticAddressResolution(final String hostname, final int port) throws Exception {
        final InetSocketAddress address = volatileAddressResolution(hostname, port).call();
        return new Callable<InetSocketAddress>() {
            @Override
            public InetSocketAddress call() {
                return address;
            }
        };
    }

    protected static Callable<InetSocketAddress> staticStatsDAddressResolution(final String hostname, final int port) throws StatsDClientException {
        try {
            return staticAddressResolution(hostname, port);
        } catch (final Exception e) {
            throw new StatsDClientException("Failed to lookup StatsD host", e);
        }
    }
}
