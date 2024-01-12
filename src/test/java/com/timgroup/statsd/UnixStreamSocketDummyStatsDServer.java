package com.timgroup.statsd;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.AbstractSelector;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Logger;
import jnr.enxio.channels.NativeSelectorProvider;
import jnr.unixsocket.UnixServerSocketChannel;
import jnr.unixsocket.UnixSocketAddress;
import jnr.unixsocket.UnixSocketChannel;

public class UnixStreamSocketDummyStatsDServer extends DummyStatsDServer {
    private final UnixServerSocketChannel server;
    private final ConcurrentLinkedQueue<UnixSocketChannel> channels = new ConcurrentLinkedQueue<>();

    private final Logger logger = Logger.getLogger(UnixStreamSocketDummyStatsDServer.class.getName());

    public UnixStreamSocketDummyStatsDServer(String socketPath) throws IOException {
        server = UnixServerSocketChannel.open();
        server.configureBlocking(false);
        server.socket().bind(new UnixSocketAddress(socketPath));
        this.accept();
        this.listen();
    }

    @Override
    protected boolean isOpen() {
        return server.isOpen();
    }

    protected void accept() {
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                final ByteBuffer packet = ByteBuffer.allocate(1500);

                while(isOpen()) {
                    if (freeze) {
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException e) {
                        }
                    } else {
                        try {
                            UnixSocketChannel clientChannel = server.accept();
                            if (clientChannel != null) {
                                clientChannel.configureBlocking(true);
                                try {
                                    logger.info("Accepted connection from " + clientChannel.getRemoteSocketAddress());
                                } catch (Exception e) {
                                    logger.warning("Failed to get remote socket address");
                                }
                                channels.add(clientChannel);
                            }
                        } catch (IOException e) {
                        }
                    }
                }
            }
        });
        thread.setDaemon(true);
        thread.start();
    }

    @Override
    protected void receive(ByteBuffer packet) throws IOException {
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
            long deadline = System.nanoTime() + 1_000L * 1_000_000L;
            while (packet.hasRemaining()) {
                read = channel.read(packet);
                if (deadline < System.nanoTime()) {
                    channel.close();
                }
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
