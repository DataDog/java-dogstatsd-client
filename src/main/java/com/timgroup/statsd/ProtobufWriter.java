package com.timgroup.statsd;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Formatter;

class ProtobufWriter {
    enum Ty {
        Varint(0),
        Double(1),
        Bytes(2);

        final byte tag;
        Ty(int tag) {
            this.tag = (byte)tag;
        }
    }

    ByteBuffer buf = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);

    void bareVarint(long value) {
        reserve(10);
        do {
            byte bv = (byte)(value & 127);
            value >>>= 7;
            if (value != 0) {
                bv |= 128;
            }
            buf.put(bv);
        }
        while (value != 0);
    }

    void bareLong(long value) {
        bareVarint((value << 1) ^ (value >> 63));
    }

    void bareDouble(double value) {
        reserve(8);
        buf.putDouble(value);
    }

    void fieldHeader(Ty ty, int id) {
        reserve(1);
        buf.put((byte)(id << 3 | ty.tag));
    }

    void fieldVarint(int id, long value) {
        fieldHeader(Ty.Varint, id);
        bareVarint(value);
    }

    void fieldDouble(int id, double value) {
        fieldHeader(Ty.Double, id);
        bareDouble(value);
    }

    void fieldPacked(int id, ProtobufWriter pw) {
        fieldHeader(Ty.Bytes, id);
        bareVarint(pw.buf.remaining());
        reserve(pw.buf.remaining());
        buf.put(pw.buf);
    }

    void reserve(int more) {
        if (buf.remaining() >= more) {
            return;
        }
        grow(more);
    }

    void grow(int more) {
        final int pos = buf.position();
        final int newSize = growSize(buf.capacity(), more);
        buf = ByteBuffer.wrap(Arrays.copyOf(buf.array(), newSize)).order(buf.order());
        buf.position(pos);
    }

    static int growSize(int capacity, int more) {
        if (capacity > Integer.MAX_VALUE - more) {
            throw new BufferOverflowException();
        }
        final int newSize = capacity + more;
        if (capacity < Integer.MAX_VALUE / 2 && newSize < capacity * 2) {
            return capacity * 2;
        }
        return newSize;
    }

    void clear() {
        buf.clear();
    }

    void flip() {
        buf.flip();
    }

    static final String b64chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    void encodeAscii(StringBuilder sb) {
        int bits = 0;
        int left = 0;
        do {
            if (left < 6 && buf.hasRemaining()) {
                int val = buf.get();
                if (val < 0) {
                    val += 256;
                }
                bits |= val << (8 - left);
                left += 8;
            }
            sb.append(b64chars.charAt(bits >> 10));
            bits = (bits << 6) & 0xffff;
            left -= 6;
        }
        while (left > 0 || buf.hasRemaining());
    }

    @Override
    public String toString() {
        Formatter fmt = new Formatter();
        fmt.format("[");
        for (int i = 0; i < buf.limit(); i++) {
            fmt.format(" %02x", buf.get(i));
        }
        fmt.format(" ]");
        return fmt.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ProtobufWriter) {
            return buf.equals(((ProtobufWriter)obj).buf);
        }
        return false;
    }
}
