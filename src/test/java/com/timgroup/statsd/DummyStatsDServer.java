
package com.timgroup.statsd;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import jnr.unixsocket.UnixDatagramChannel;
import jnr.unixsocket.UnixSocketAddress;
import java.nio.charset.StandardCharsets;


final class DummyStatsDServer {
    private final Queue<String> messagesReceived = new LinkedBlockingQueue<String>();
    private final DatagramChannel server;
    private volatile Boolean freeze = false;

    public DummyStatsDServer(int port) throws IOException {
        server = DatagramChannel.open();
        server.bind(new InetSocketAddress(port));
        this.listen();
    }

    public DummyStatsDServer(String socketPath) throws IOException {
        server = UnixDatagramChannel.open();
        server.bind(new UnixSocketAddress(socketPath));
        this.listen();
    }

    private void listen() {
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                final ByteBuffer packet = ByteBuffer.allocate(1500);
                while(server.isOpen()) {
                    if (freeze) {
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException e) {
                        }
                    } else {
                        try {
                            packet.clear();
                            server.receive(packet);
                            packet.flip();
                            for (String msg : StandardCharsets.UTF_8.decode(packet).toString().split("\n")) {
                                messagesReceived.add(msg.trim());
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

    public void waitForMessage() {
        while (messagesReceived.isEmpty()) {
            try {
                Thread.sleep(50L);
            } catch (InterruptedException e) {
            }
        }
    }

    public List<String> messagesReceived() {
        return new ArrayList<String>(messagesReceived);
    }

    public void freeze() {
        freeze = true;
    }

    public void unfreeze() {
        freeze = false;
    }

    public void close() throws IOException {
        try {
            server.close();
        } catch (Exception e) {
            //ignore
        }
    }

    public void clear() {
        messagesReceived.clear();
    }

}
