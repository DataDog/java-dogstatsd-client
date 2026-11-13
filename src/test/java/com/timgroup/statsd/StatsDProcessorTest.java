package com.timgroup.statsd;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
    public void writes_multibyte_message_that_exactly_fills_the_packet() throws Exception {
        // One character short of the limit, but 'é' takes two bytes in UTF-8.
        String packet = packet("é", MAX_PACKET_SIZE - 1);

        assertEquals(Arrays.asList(packet), process(new ChunkedMessage(packet), 1));
    }

    @Test(timeout = 5000L)
    public void writes_every_chunk_when_one_exactly_fills_the_packet() throws Exception {
        String first = packet("", MAX_PACKET_SIZE);
        String second = packet("", 10);

        assertEquals(Arrays.asList(first, second), process(new ChunkedMessage(first, second), 2));
    }

    @Test(timeout = 5000L)
    public void writes_message_that_exactly_fills_the_rest_of_a_packet() throws Exception {
        String first = packet("", 20);
        String second = packet("", MAX_PACKET_SIZE - 20);

        assertEquals(Arrays.asList(first + second), process(new ChunkedMessage(first, second), 1));
    }

    @Test(timeout = 5000L)
    public void rejects_multibyte_message_longer_than_the_packet() throws Exception {
        // Exactly MAX_PACKET_SIZE chars, but 'é' takes two bytes in UTF-8.
        processor = start();
        assertTrue(processor.send(new ChunkedMessage(packet("é", MAX_PACKET_SIZE))));

        while (errors.getExceptions().isEmpty()) {
            Thread.sleep(10L);
        }
        assertEquals(InvalidMessageException.class, errors.getExceptions().get(0).getClass());
        assertEquals(null, processor.getOutboundQueue().poll());
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
