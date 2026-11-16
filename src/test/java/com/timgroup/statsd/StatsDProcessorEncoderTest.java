package com.timgroup.statsd;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.junit.Test;

public class StatsDProcessorEncoderTest {

    private static final int MAX_PACKET_SIZE = 64;

    private final StatsDProcessor.Encoder encoder = new StatsDProcessor.Encoder(MAX_PACKET_SIZE);
    private final ByteBuffer sendBuffer = ByteBuffer.allocate(MAX_PACKET_SIZE);

    @Test
    public void encodes_message_that_exactly_fills_the_packet() {
        assertEncodesExactFit(message("", MAX_PACKET_SIZE));
    }

    @Test
    public void encodes_two_byte_chars_that_exactly_fill_the_packet() {
        assertEncodesExactFit(message("é", MAX_PACKET_SIZE - 1));
    }

    @Test
    public void encodes_three_byte_chars_that_exactly_fill_the_packet() {
        assertEncodesExactFit(message("€", MAX_PACKET_SIZE - 2));
    }

    @Test
    public void encodes_surrogate_pair_that_exactly_fills_the_packet() {
        // Two chars, four bytes.
        assertEncodesExactFit(message("😀", MAX_PACKET_SIZE - 2));
    }

    @Test
    public void rejects_message_one_byte_longer_than_the_packet() {
        assertRejects(message("", MAX_PACKET_SIZE + 1));
    }

    @Test
    public void rejects_multibyte_message_one_byte_longer_than_the_packet() {
        // As many chars as fit in a packet, but 'é' takes two bytes.
        assertRejects(message("é", MAX_PACKET_SIZE));
    }

    @Test
    public void rejects_message_longer_than_the_space_left() {
        sendBuffer.put(new byte[10]);

        assertRejects(message("", MAX_PACKET_SIZE - 9));
    }

    /** {@code length} chars: {@code head}, then ASCII filler. */
    private static String message(String head, int length) {
        char[] filler = new char[length - head.length()];
        Arrays.fill(filler, 'x');
        return head + new String(filler);
    }

    private void assertEncodesExactFit(String message) {
        assertEquals(MAX_PACKET_SIZE, message.getBytes(StandardCharsets.UTF_8).length);

        encoder.encode(new StringBuilder(message), sendBuffer);

        sendBuffer.flip();
        assertEquals(message, StandardCharsets.UTF_8.decode(sendBuffer).toString());
    }

    private void assertRejects(String message) {
        int position = sendBuffer.position();
        try {
            encoder.encode(new StringBuilder(message), sendBuffer);
            fail("expected BufferOverflowException");
        } catch (BufferOverflowException expected) {
            // Nothing is written on overflow.
            assertEquals(position, sendBuffer.position());
        }
    }
}
