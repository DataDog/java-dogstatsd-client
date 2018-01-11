package com.timgroup.statsd;

import java.io.IOException;
import java.util.List;

/**
 * Protocol implementation that just put the message in a list. Used for testing.
 */
class ListProtocol implements Protocol {
    final List<String> messageReceived;

    ListProtocol(List<String> messageReceived) {
        this.messageReceived = messageReceived;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void send(String message) throws IOException {
        messageReceived.add(message);
    }

    @Override
    public void flush() throws IOException {

    }
}
