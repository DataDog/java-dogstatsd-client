package com.timgroup.statsd;

import static com.timgroup.statsd.NonBlockingStatsDClient.DEFAULT_UDS_MAX_PACKET_SIZE_BYTES;

import java.io.Closeable;
import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

abstract class DummyStatsDServer implements Closeable {
    private final List<String> messagesReceived = new ArrayList<String>();
    private AtomicInteger packetsReceived = new AtomicInteger(0);

    protected volatile Boolean freeze = false;
    Thread thread;

    protected void listen() {
        thread =
                new Thread(
                        new Runnable() {
                            @Override
                            public void run() {
                                final ByteBuffer packet =
                                        ByteBuffer.allocate(DEFAULT_UDS_MAX_PACKET_SIZE_BYTES);

                                while (isOpen() && !Thread.interrupted()) {
                                    if (freeze) {
                                        try {
                                            Thread.sleep(10);
                                        } catch (InterruptedException e) {
                                        }
                                    } else {
                                        try {
                                            ((Buffer) packet)
                                                    .clear(); // Cast necessary to handle Java9
                                            // covariant return types
                                            // see: https://jira.mongodb.org/browse/JAVA-2559 for
                                            // ref.
                                            receive(packet);
                                            handlePacket(packet);
                                        } catch (IOException e) {
                                        }
                                    }
                                }
                            }
                        });
        thread.setDaemon(true);
        thread.start();
    }

    protected boolean sleepIfFrozen() {
        if (freeze) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
            }
        }
        return freeze;
    }

    protected void handlePacket(ByteBuffer packet) {
        packetsReceived.addAndGet(1);

        packet.flip();
        for (String msg : StandardCharsets.UTF_8.decode(packet).toString().split("\n")) {
            addMessage(msg);
        }
    }

    public void waitForMessage() {
        waitForMessage(null);
    }

    public void waitForMessage(String prefix) {
        boolean done = false;

        while (!done) {
            try {
                synchronized (messagesReceived) {
                    done = !messagesReceived.isEmpty();
                }

                if (done && prefix != null && !prefix.isEmpty()) {
                    done = false;
                    List<String> messages = this.messagesReceived();
                    for (String message : messages) {
                        if (message.contains(prefix)) {
                            return;
                        }
                    }
                }
                Thread.sleep(100L);
            } catch (InterruptedException e) {
            }
        }
    }

    public List<String> messagesReceived() {
        synchronized (messagesReceived) {
            return new ArrayList<String>(messagesReceived);
        }
    }

    public int getPacketsReceived() {
        return packetsReceived.get();
    }

    public void freeze() {
        freeze = true;
    }

    public void unfreeze() {
        freeze = false;
    }

    public void clear() {
        packetsReceived.set(0);
        messagesReceived.clear();
    }

    protected abstract boolean isOpen();

    protected abstract void receive(ByteBuffer packet) throws IOException;

    protected void addMessage(String msg) {
        synchronized (messagesReceived) {
            String trimmed = msg.trim();
            if (!trimmed.isEmpty()) {
                messagesReceived.add(msg.trim());
            }
        }
    }
}
