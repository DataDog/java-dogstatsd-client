package com.timgroup.statsd;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.AbstractSelector;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import jnr.enxio.channels.NativeSelectorProvider;
import jnr.unixsocket.UnixServerSocketChannel;
import jnr.unixsocket.UnixSocketAddress;
import jnr.unixsocket.UnixSocketChannel;

public class UnixStreamSocketDummyStatsDServer extends DummyStatsDServer {
    private final UnixServerSocketChannel server;
    private final List<UnixSocketChannel> channels = new ArrayList<>();

    private final Logger logger = Logger.getLogger(UnixStreamSocketDummyStatsDServer.class.getName());

    public UnixStreamSocketDummyStatsDServer(String socketPath) throws IOException {
        server = UnixServerSocketChannel.open();
        server.configureBlocking(false);
        server.socket().bind(new UnixSocketAddress(socketPath));
        this.listen();
    }
    @Override
    protected boolean isOpen() {
        return server.isOpen();
    }

    @Override
    protected void receive(ByteBuffer packet) throws IOException {
        UnixSocketChannel clientChannel = server.accept();
        if (clientChannel != null) {
            clientChannel.configureBlocking(false);
            logger.info("Accepted connection from " + clientChannel.getRemoteSocketAddress());
            channels.add(clientChannel);
        }

        for (UnixSocketChannel channel : channels) {
            if (channel.isConnected()) {
                if (readPacket(channel, packet)) {
                    return;
                }
            }
        }

    }

    private boolean readPacket(SocketChannel channel, ByteBuffer packet) throws IOException {
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
            while (packet.hasRemaining()) {
                read = channel.read(packet);
            }
            return true;
        } catch (IOException e) {
            channel.close();
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
            //ignore
        }
    }

}
