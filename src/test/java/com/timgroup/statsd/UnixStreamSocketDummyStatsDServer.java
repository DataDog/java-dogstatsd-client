package com.timgroup.statsd;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.net.SocketAddress;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Logger;
import jnr.unixsocket.UnixServerSocketChannel;
import jnr.unixsocket.UnixSocketAddress;
import jnr.unixsocket.UnixSocketChannel;

import static com.timgroup.statsd.NonBlockingStatsDClient.DEFAULT_UDS_MAX_PACKET_SIZE_BYTES;

public class UnixStreamSocketDummyStatsDServer extends DummyStatsDServer {
    private final Object server; // Object is either ServerSocketChannel or UnixServerSocketChannel
    private final ConcurrentLinkedQueue<SocketChannel> channels = new ConcurrentLinkedQueue<>();
    private final boolean useNativeUds;

    private final Logger logger = Logger.getLogger(UnixStreamSocketDummyStatsDServer.class.getName());

    public UnixStreamSocketDummyStatsDServer(String socketPath) throws IOException {
        this.useNativeUds = ClientChannelUtils.hasNativeUdsSupport();
        if (useNativeUds) {
            try {
                Class<?> udsAddressClass = Class.forName("java.net.UnixDomainSocketAddress");
                Object udsAddress = udsAddressClass.getMethod("of", String.class).invoke(null, socketPath);
                
                ServerSocketChannel nativeServer = ServerSocketChannel.open();
                nativeServer.bind((SocketAddress) udsAddress);
                this.server = nativeServer;
            } catch (ReflectiveOperationException e) {
                throw new IOException(e);
            }
        } else {
            UnixServerSocketChannel jnrServer = UnixServerSocketChannel.open();
            jnrServer.configureBlocking(true);
            jnrServer.socket().bind(new UnixSocketAddress(socketPath));
            this.server = jnrServer;
        }
        this.listen();
    }

    @Override
    protected boolean isOpen() {
        return useNativeUds ? ((ServerSocketChannel)server).isOpen() : ((UnixServerSocketChannel)server).isOpen();
    }

    @Override
    protected void receive(ByteBuffer packet) throws IOException {
        // This is unused because we re-implement listen() to fit our needs
    }

    @Override
    protected void listen() {
        try {
            String localAddressMessage = useNativeUds ? "Listening on " + ((ServerSocketChannel)server).getLocalAddress() : "Listening on " + ((UnixServerSocketChannel)server).getLocalSocketAddress();
            logger.info(localAddressMessage);
        } catch (Exception e) {
            logger.warning("Failed to get local address: " + e);
        }
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                while(isOpen()) {
                    if (sleepIfFrozen()) {
                        continue;
                    }
                    try {
                        logger.info("Waiting for connection");
                        SocketChannel clientChannel = null;
                        clientChannel = useNativeUds ? ((ServerSocketChannel)server).accept() : ((UnixServerSocketChannel)server).accept();
                        if (clientChannel != null) {
                            clientChannel.configureBlocking(true);
                            String connectionMessage = useNativeUds ? "Accepted connection from " + clientChannel.getRemoteAddress() : "Accepted connection from " + ((UnixSocketChannel)clientChannel).getRemoteSocketAddress();
                            logger.info(connectionMessage);
                            channels.add(clientChannel);
                            readChannel(clientChannel);
                        }
                    } catch (Exception e) {
                        // ignore
                    }
                }
            }
        });
        thread.setDaemon(true);
        thread.start();
    }

    public void readChannel(final SocketChannel clientChannel) {
        logger.info("Reading from " + clientChannel);
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                final ByteBuffer packet = ByteBuffer.allocate(DEFAULT_UDS_MAX_PACKET_SIZE_BYTES);

                while(clientChannel.isOpen()) {
                    if (sleepIfFrozen()) {
                        continue;
                    }
                    ((Buffer)packet).clear();  // Cast necessary to handle Java9 covariant return types
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
            ByteBuffer delimiterBuffer = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).order(ByteOrder.LITTLE_ENDIAN);

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
            if (useNativeUds) {
                ((ServerSocketChannel)server).close();
            } else {
                ((UnixServerSocketChannel)server).close();
            }
            for (SocketChannel channel : channels) {
                channel.close();
            }
        } catch (Exception e) {
            //ignore
        }
    }
}
