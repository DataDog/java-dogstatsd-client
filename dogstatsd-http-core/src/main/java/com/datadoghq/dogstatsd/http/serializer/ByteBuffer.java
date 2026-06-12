/* Unless explicitly stated otherwise all files in this repository are
 * licensed under the Apache 2.0 License.
 *
 * This product includes software developed at Datadog
 *  (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.
 */

package com.datadoghq.dogstatsd.http.serializer;

import java.util.Arrays;

class ByteBuffer extends Buffer {
    byte[] data;

    ByteBuffer() {
        data = new byte[64];
    }

    ByteBuffer(int cap) {
        data = new byte[cap];
    }

    void put(byte v) {
        reserve(1);
        data[size++] = v;
    }

    void put(byte[] vs, int length) {
        reserve(length);
        System.arraycopy(vs, 0, data, size, length);
        size += length;
    }

    void put(byte[] vs) {
        put(vs, vs.length);
    }

    void put(ByteBuffer buf) {
        put(buf.data, buf.size);
    }

    void putFixed32(int v) {
        put((byte) v);
        put((byte) (v >>> 8));
        put((byte) (v >>> 16));
        put((byte) (v >>> 24));
    }

    void putFixed64(long v) {
        put((byte) v);
        put((byte) (v >>> 8));
        put((byte) (v >>> 16));
        put((byte) (v >>> 24));
        put((byte) (v >>> 32));
        put((byte) (v >>> 40));
        put((byte) (v >>> 48));
        put((byte) (v >>> 56));
    }

    void putFloat32(float v) {
        putFixed32(Float.floatToRawIntBits(v));
    }

    void putFloat64(double v) {
        putFixed64(Double.doubleToRawLongBits(v));
    }

    void putUint64(long v) {
        do {
            put((byte) (v & 127 | (v > 127 ? 128 : 0)));
            v >>>= 7;
        } while (v != 0);
    }

    void putSint64(long v) {
        putUint64((v >> 63) ^ (v << 1));
    }

    void putBytesFieldHeader(int id, int len) {
        putUint64(ProtoUtil.bytesFieldHeader(id));
        putUint64(len);
    }

    void putBytesField(int id, ByteBuffer data) {
        putBytesFieldHeader(id, data.length());
        put(data);
    }

    @Override
    int capacity() {
        return data.length;
    }

    @Override
    protected void realloc(int newSize) {
        data = Arrays.copyOf(data, newSize);
    }

    byte[] toArray() {
        if (size == data.length) {
            return data;
        }
        return Arrays.copyOf(data, size);
    }
}
