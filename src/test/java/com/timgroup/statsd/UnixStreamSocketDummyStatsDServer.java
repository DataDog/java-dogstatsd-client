package com.timgroup.statsd;

import static com.timgroup.statsd.NonBlockingStatsDClient.DEFAULT_UDS_MAX_PACKET_SIZE_BYTES;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Logger;
import jnr.unixsocket.UnixServerSocketChannel;
import jnr.unixsocket.UnixSocketAddress;
import jnr.unixsocket.UnixSocketChannel;

public class UnixStreamSocketDummyStatsDServer extends DummyStatsDServer {
    private final UnixServerSocketChannel server;
    private final ConcurrentLinkedQueue<UnixSocketChannel> channels = new ConcurrentLinkedQueue<>();

    private final Logger logger =
            Logger.getLogger(UnixStreamSocketDummyStatsDServer.class.getName());

    public UnixStreamSocketDummyStatsDServer(String socketPath) throws IOException {
        server = UnixServerSocketChannel.open();
        server.configureBlocking(true);
        UnixSocketAddress address = new UnixSocketAddress(socketPath);
        System.out.println("========== Server bind address: " + address);
        System.out.println("========== Server bind address type: " + address.getClass().getName());
        server.socket().bind(address);
        this.listen();
    }

    @Override
    protected boolean isOpen() {
        return server.isOpen();
    }

    @Override
    protected void receive(ByteBuffer packet) throws IOException {
        // This is unused because we re-implement listen() to fit our needs
    }

    @Override
    protected void listen() {
        System.out.println("========== Server local address: " + server.getLocalSocketAddress());
        System.out.println("========== Server local address type: " + server.getLocalSocketAddress().getClass().getName());
        logger.info("Listening on " + server.getLocalSocketAddress());
        Thread thread =
                new Thread(
                        new Runnable() {
                            @Override
                            public void run() {
                                while (isOpen()) {
                                    if (sleepIfFrozen()) {
                                        continue;
                                    }
                                    try {
                                        logger.info("Waiting for connection");
                                        UnixSocketChannel clientChannel = server.accept();
                                        if (clientChannel != null) {
                                            clientChannel.configureBlocking(true);
                                            try {
                                                System.out.println("========== Client remote address: " + clientChannel.getRemoteSocketAddress());
                                                System.out.println("========== Client remote address type: " + clientChannel.getRemoteSocketAddress().getClass().getName());
                                                logger.info(
                                                        "Accepted connection from "
                                                                + clientChannel
                                                                        .getRemoteSocketAddress());
                                            } catch (Exception e) {
                                                logger.warning(
                                                        "Failed to get remote socket address");
                                            }
                                            channels.add(clientChannel);
                                            readChannel(clientChannel);
                                        }
                                    } catch (IOException e) {
                                    }
                                }
                            }
                        });
        thread.setDaemon(true);
        thread.start();
    }

    public void readChannel(final UnixSocketChannel clientChannel) {
        logger.info("Reading from " + clientChannel);
        Thread thread =
                new Thread(
                        new Runnable() {
                            @Override
                            public void run() {
                                final ByteBuffer packet =
                                        ByteBuffer.allocate(DEFAULT_UDS_MAX_PACKET_SIZE_BYTES);

                                while (clientChannel.isOpen()) {
                                    if (sleepIfFrozen()) {
                                        continue;
                                    }
                                    ((Buffer) packet)
                                            .clear(); // Cast necessary to handle Java9 covariant
                                    // return types
                                    // see: https://jira.mongodb.org/browse/JAVA-2559 for ref.
                                    if (readPacket(clientChannel, packet)) {
                                        handlePacket(packet);
                                    } else {
                                        try {
                                            clientChannel.close();
                                        } catch (IOException e) {
                                            logger.warning("Failed to close channel: " + e);
                                        }
                                    }
                                }
                                logger.info("Disconnected from " + clientChannel);
                            }
                        });
        thread.setDaemon(true);
        thread.start();
    }

    private boolean readPacket(SocketChannel channel, ByteBuffer packet) {
        try {
            ByteBuffer delimiterBuffer =
                    ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).order(ByteOrder.LITTLE_ENDIAN);

            int read = channel.read(delimiterBuffer);

            delimiterBuffer.flip();
            if (read <= 0) {
                // There was nothing to read
                return false;
            }

            int packetSize = delimiterBuffer.getInt();
            if (packetSize > packet.capacity()) {
                throw new IOException("Packet size too large");
            }

            packet.limit(packetSize);
            while (packet.hasRemaining() && channel.isConnected()) {
                channel.read(packet);
            }
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    public void close() throws IOException {
        try {
            server.close();
            for (UnixSocketChannel channel : channels) {
                channel.close();
            }
        } catch (Exception e) {
            // ignore
        }
    }
}
