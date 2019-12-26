
package com.timgroup.statsd;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.ArrayList;
import java.util.List;
import jnr.unixsocket.UnixDatagramChannel;
import jnr.unixsocket.UnixSocketAddress;
import java.nio.charset.StandardCharsets;


class DummyLowMemStatsDServer extends DummyStatsDServer {
    private final AtomicInteger packetCount = new AtomicInteger(0);
    private final AtomicInteger messageCount = new AtomicInteger(0);

    public DummyLowMemStatsDServer(int port) throws IOException {
        super(port);
    }

    public DummyLowMemStatsDServer(String socketPath) throws IOException {
        super(socketPath);
    }

    @Override
    protected void listen() {
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                final ByteBuffer packet = ByteBuffer.allocateDirect(1500);

                while(server.isOpen()) {
                    if (freeze) {
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException e) {
                        }
                    } else {
                        try {
                            ((Buffer)packet).clear();  // Cast necessary to handle Java9 covariant return types
                                                       // see: https://jira.mongodb.org/browse/JAVA-2559 for ref.
                            server.receive(packet);
                            packet.flip();
                            packetCount.incrementAndGet();

                            for (String msg : StandardCharsets.UTF_8.decode(packet).toString().split("\n")) {
                                messageCount.incrementAndGet();
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
    public void clear() {
        packetCount.set(0);
        messageCount.set(0);
        super.clear();
    }

    public int getPacketCount() {
        return packetCount.get();
    }

    public int getMessageCount() {
        return messageCount.get();
    }

}
