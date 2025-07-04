package com.timgroup.statsd;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

import static java.lang.Character.MIN_SURROGATE;
import static org.junit.Assert.assertEquals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class Utf8Test {

    @Test
    public void should_handle_malformed_inputs() throws CharacterCodingException {
        shouldHandleMalformedInput("foo" + MIN_SURROGATE + "bar");
        shouldHandleMalformedInput("🍻☀️😎🏖️" + MIN_SURROGATE + "🍻☀️😎🏖️");
    }

    private static void shouldHandleMalformedInput(String malformedInput) throws CharacterCodingException {
        CharsetEncoder utf8Encoder = StandardCharsets.UTF_8.newEncoder()
                .onMalformedInput(CodingErrorAction.REPLACE)
                .onUnmappableCharacter(CodingErrorAction.REPLACE);
        ByteBuffer encoded = utf8Encoder.encode(CharBuffer.wrap(malformedInput));

        assertThat(Utf8.encodedLength(malformedInput), equalTo(encoded.limit()));
    }

    @Test
    public void sanitize() {
        assertEquals("abc", Utf8.sanitize("abc"));
        assertEquals("abc", Utf8.sanitize("a\nb|c"));
        assertEquals("abc", Utf8.sanitize("a\t\027b\000c"));
        assertEquals("🍻☀️😎🏖️", Utf8.sanitize("🍻☀️😎🏖️"));
    }
}
