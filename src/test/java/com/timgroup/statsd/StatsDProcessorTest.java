package com.timgroup.statsd;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Test;

public class StatsDProcessorTest {

    private static final int MAX_PACKET_SIZE = 64;

    private final RecordingErrorHandler errors = new RecordingErrorHandler();
    private StatsDProcessor processor;

    @After
    public void stop() throws InterruptedException {
        if (processor != null) {
            processor.shutdown(false);
        }
    }

    @Test(timeout = 5000L)
    public void writes_message_that_exactly_fills_the_packet() throws Exception {
        String packet = packet("", MAX_PACKET_SIZE);

        assertEquals(Arrays.asList(packet), process(new ChunkedMessage(packet), 1));
    }

    @Test(timeout = 5000L)
    public void writes_message_that_exactly_fills_the_rest_of_a_packet() throws Exception {
        String first = packet("", 20);
        String second = packet("", MAX_PACKET_SIZE - 20);

        assertEquals(Arrays.asList(first + second), process(new ChunkedMessage(first, second), 1));
    }

    @Test(timeout = 5000L)
    public void reports_multibyte_overflow_of_an_empty_buffer() throws Exception {
        // Fewer chars than fit in a packet, but more bytes: 'é' takes two bytes in UTF-8.
        assertEquals(
                Arrays.<String>asList(), processOverflow(new ChunkedMessage(multibyte(40)), 0));
    }

    @Test(timeout = 5000L)
    public void reports_multibyte_overflow_after_a_buffered_chunk() throws Exception {
        String first = packet("", 20);

        assertEquals(
                Arrays.asList(first), processOverflow(new ChunkedMessage(first, multibyte(40)), 1));
    }

    /** {@code length} chars, mostly two-byte 'é', ending with a newline. */
    private static String multibyte(int length) {
        char[] chars = new char[length - 1];
        Arrays.fill(chars, 'é');
        return new String(chars) + "\n";
    }

    /**
     * Processes a message that overflows a packet, and returns the packets written before the
     * overflow.
     */
    private List<String> processOverflow(Message message, int expectedPackets) throws Exception {
        processor = start();
        assertTrue(processor.send(message));

        while (errors.getExceptions().isEmpty()) {
            Thread.sleep(10L);
        }
        // The client tests rely on this type: InvalidMessageException means the message was
        // rejected before it was encoded.
        assertEquals(BufferOverflowException.class, errors.getExceptions().get(0).getClass());

        List<String> packets = new ArrayList<>();
        ByteBuffer buffer;
        while ((buffer = processor.getOutboundQueue().poll()) != null) {
            if (buffer.position() == 0) {
                throw new AssertionError("processor flushed an empty buffer");
            }
            buffer.flip();
            packets.add(StandardCharsets.UTF_8.decode(buffer).toString());
        }
        assertEquals(expectedPackets, packets.size());
        return packets;
    }

    /** A packet of {@code length} chars that starts with {@code head} and ends with a newline. */
    private static String packet(String head, int length) {
        char[] filler = new char[length - head.length() - 1];
        Arrays.fill(filler, 'x');
        return head + new String(filler) + "\n";
    }

    private StatsDProcessor start() throws Exception {
        StatsDProcessor started =
                new StatsDNonBlockingProcessor(
                        16, errors, MAX_PACKET_SIZE, 8, 1, 0, 1, Executors.defaultThreadFactory());
        started.startWorkers("StatsDProcessorTest-");
        return started;
    }

    private List<String> process(Message message, int expectedPackets) throws Exception {
        processor = start();
        assertTrue(processor.send(message));

        List<String> packets = new ArrayList<>();
        while (packets.size() < expectedPackets) {
            List<Exception> exceptions = errors.getExceptions();
            if (!exceptions.isEmpty()) {
                throw new AssertionError("processor reported errors: " + exceptions);
            }
            ByteBuffer buffer = processor.getOutboundQueue().poll(10, TimeUnit.MILLISECONDS);
            if (buffer == null) {
                continue;
            }
            if (buffer.position() == 0) {
                throw new AssertionError("processor flushed an empty buffer");
            }
            buffer.flip();
            packets.add(StandardCharsets.UTF_8.decode(buffer).toString());
        }
        return packets;
    }

    /** Writes one prepared chunk per call, like a multi-valued message split across packets. */
    private static final class ChunkedMessage extends Message {
        private final String[] chunks;
        private int next;

        ChunkedMessage(String... chunks) {
            super(Message.Type.DISTRIBUTION, TagsCardinality.DEFAULT);
            this.chunks = chunks;
        }

        @Override
        boolean writeTo(StringBuilder builder, int capacity) {
            builder.append(chunks[next++]);
            return next < chunks.length;
        }

        @Override
        public void aggregate(Message message) {}
    }
}
