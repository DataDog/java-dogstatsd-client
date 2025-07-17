package com.timgroup.statsd;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

class DummyLowMemStatsDServer extends UDPDummyStatsDServer {
    private final AtomicInteger messageCount = new AtomicInteger(0);

    public DummyLowMemStatsDServer(int port) throws IOException {
        super(port);
    }

    @Override
    public void clear() {
        messageCount.set(0);
        super.clear();
    }

    public int getMessageCount() {
        return messageCount.get();
    }

    @Override
    protected void addMessage(String msg) {
        messageCount.incrementAndGet();
    }
}
