package com.timgroup.statsd;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static com.timgroup.statsd.DirectSketch.key;

public class DirectSketchTest {
    DirectSketch s = new DirectSketch();

    @Test
    public void keys() {
        final double oneSmaller = Double.longBitsToDouble(Double.doubleToLongBits(1e-9)-1);
        assertEquals(32767, key(1e300));
        assertEquals(DirectSketch.bias, key(1));
        assertEquals(0, key(oneSmaller));
        assertEquals(0, key(0));
        assertEquals(0, key(-oneSmaller));
        assertEquals(-DirectSketch.bias, key(-1));
        assertEquals(-32767, key(-1e300));
    }

    @Test
    public void basic() {
        s.build((long[])null, 1);
        assertEquals(0, s.min, 0);
        assertEquals(0, s.max, 0);
        assertEquals(0, s.sum, 0);
        assertEquals(0, s.cnt, 0);
        assertEquals(longs(), s.keys);
        assertEquals(varints(), s.bins);

        s.build(new long[]{423, 234}, 0);
        assertEquals(234, s.min, 0);
        assertEquals(423, s.max, 0);
        assertEquals(657, s.sum, 0);
        assertEquals(2, s.cnt, 0);
        assertEquals(longs(1690, 1728), s.keys);
        assertEquals(varints(1, 1), s.bins);
        ProtobufWriter pw = new ProtobufWriter();
        s.serialize(pw, 1747236777);
        StringBuilder sb = new StringBuilder();
        pw.flip();
        pw.encodeAscii(sb);
        assertEquals("CKnvksEGEAIZAAAAAABAbUAhAAAAAABwekApAAAAAACIdEAxAAAAAACIhEA6BLQagBtCAgEB", sb.toString());

        s.build(new long[]{}, 1);
        assertEquals(0, s.min, 0);
        assertEquals(0, s.max, 0);
        assertEquals(0, s.sum, 0);
        assertEquals(0, s.cnt, 0);
        assertEquals(longs(), s.keys);
        assertEquals(varints(), s.bins);

        long[] values = new long[]{4, 2, 3, 1, 0, 3, -1, -2};
        s.build(values, 2);

        assertEquals(-2, s.min, 0);
        assertEquals(4, s.max, 0);
        assertEquals(10, s.sum, 0);
        assertEquals((double)values.length, s.cnt, 0);

        assertEquals(longs(-1383, -1338, 0, 1338, 1383, 1409, 1427), s.keys);
        assertEquals(varints(1, 1, 1, 1, 1, 2, 1), s.bins);

        ProtobufWriter w = new ProtobufWriter();
        s.serialize(w, 1000);
        w.flip();
        assertEquals("["+
            " 08 e8 07" +
            " 10 08" +
            " 19 00 00 00 00 00 00 00 c0" +
            " 21 00 00 00 00 00 00 10 40" +
            " 29 00 00 00 00 00 00 f4 3f" +
            " 31 00 00 00 00 00 00 24 40" +
            " 3a 0d cd 15 f3 14 00 f4 14 ce 15 82 16 a6 16" +
            " 42 07 01 01 01 01 01 02 01" +
            " ]", w.toString());
    }

    @Test
    public void overflow() {
        final int maxInt = Integer.MAX_VALUE;

        long[] values = new long[]{ 1, 1, 1, 1, 1, 1 };
        s.build(values, 1e-9);

        assertEquals(0, s.min, 1);
        assertEquals(0, s.max, 1);
        assertEquals((double)values.length * (1 / 1e-9), s.cnt, 0);
        assertEquals(longs(1338, 1338, 1338), s.keys);
        assertEquals(varints(maxInt, maxInt, 1705032700), s.bins);

        s.build(values, 1/(double)(maxInt));
        assertEquals(longs(1338, 1338, 1338, 1338, 1338, 1338), s.keys);
        assertEquals(varints(maxInt, maxInt, maxInt, maxInt, maxInt, maxInt), s.bins);
    }

    ProtobufWriter varints(long... vals) {
        ProtobufWriter w = new ProtobufWriter();
        for (long v : vals) {
            w.bareVarint(v);
        }
        w.flip();
        return w;
    }

    ProtobufWriter longs(long... vals) {
        ProtobufWriter w = new ProtobufWriter();
        for (long v : vals) {
            w.bareLong(v);
        }
        w.flip();
        return w;
    }
}
