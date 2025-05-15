package com.timgroup.statsd;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ProtobufWriterTest {
    ProtobufWriter w = new ProtobufWriter();

    @Test
    public void varint() {
        String[] cases = new String[]{
            "                 0 [ 00 ]",
            "                 1 [ 01 ]",
            "               128 [ 80 01 ]",
            "               129 [ 81 01 ]",
            "               255 [ ff 01 ]",
            "               256 [ 80 02 ]",
            "             16384 [ 80 80 01 ]",
            "        0x7fffffff [ ff ff ff ff 07 ]",
            "        0x80000000 [ 80 80 80 80 08 ]",
            "0x7fffffffffffffff [ ff ff ff ff ff ff ff ff 7f ]"
        };

        for (String c : cases) {
            String[] pair = c.trim().split(" +", 2);

            w.clear();
            w.bareVarint(Long.decode(pair[0]));
            w.flip();
            assertEquals(c, pair[1], w.toString());
        }
    }

    @Test
    public void zigzag() {
        String[] cases = new String[] {
            "                  0 [ 00 ]",
            "                 -1 [ 01 ]",
            "                  1 [ 02 ]",
            "                 -2 [ 03 ]",
            "                  2 [ 04 ]",
            "               -128 [ ff 01 ]",
            "                128 [ 80 02 ]",
            "         0x7fffffff [ fe ff ff ff 0f ]",
            "        -0x80000000 [ ff ff ff ff 0f ]",
            " 0x7fffffffffffffff [ fe ff ff ff ff ff ff ff ff 01 ]",
            "-0x8000000000000000 [ ff ff ff ff ff ff ff ff ff 01 ]",
        };

        for (String c : cases) {
            String[] pair = c.trim().split(" +", 2);

            w.clear();
            w.bareLong(Long.decode(pair[0]));
            w.flip();
            assertEquals(c, pair[1], w.toString());
        }
    }

    @Test
    public void fields() {
        ProtobufWriter v = new ProtobufWriter();
        v.bareLong(1);
        v.bareLong(2);
        v.bareLong(3);
        v.flip();

        w.clear();
        w.fieldVarint(1, 256);
        w.fieldDouble(2, 3.14);
        w.fieldPacked(3, v);
        w.flip();

        assertEquals("[ 08 80 02 11 1f 85 eb 51 b8 1e 09 40 1a 03 02 04 06 ]", w.toString());

        StringBuilder b = new StringBuilder();
        w.encodeAscii(b);
        assertEquals("CIACER+F61G4HglAGgMCBAY", b.toString());
    }

    @Test
    public void varintReserve() {
        w.buf.position(w.buf.limit());
        w.bareVarint(Long.MIN_VALUE);
    }

    @Test
    public void growSize() {
        assertEquals(16, ProtobufWriter.growSize(8, 4));
        assertEquals(100, ProtobufWriter.growSize(8, 92));
        assertEquals(0x4000_0000, ProtobufWriter.growSize(0x2000_0000, 4));
        assertEquals(0x4000_1000, ProtobufWriter.growSize(0x4000_0000, 0x1000));

        Exception ex = null;
        try {
                ProtobufWriter.growSize(0x4000_0000, 0x4000_0000);
        } catch (BufferOverflowException e) {
            ex = e;
        }
        assertNotNull(ex);
    }

}
