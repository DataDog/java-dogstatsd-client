/* Unless explicitly stated otherwise all files in this repository are
 * licensed under the Apache 2.0 License.
 *
 * This product includes software developed at Datadog
 *  (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.
 */

package com.datadoghq.dogstatsd.http.serializer;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.nio.BufferOverflowException;

class ColumnarBuffer {
    final ByteBuffer[] buffers = new ByteBuffer[Column.MAX + 1];

    ByteBuffer column(Column dst) {
        int idx = dst.id;
        if (buffers[idx] == null) {
            buffers[idx] = new ByteBuffer();
        }
        return buffers[idx];
    }

    void clear() {
        for (ByteBuffer b : buffers) {
            if (!Buffer.isEmpty(b)) {
                b.clear();
            }
        }
    }

    void clear(Column dst) {
        column(dst).clear();
    }

    void put(ColumnarBuffer other) {
        for (int i = 0; i < buffers.length; i++) {
            if (Buffer.isEmpty(other.buffers[i])) {
                continue;
            }
            if (buffers[i] == null) {
                buffers[i] = new ByteBuffer();
            }
            buffers[i].put(other.buffers[i]);
        }
    }

    void putBytes(Column dst, byte[] val) {
        putUint64(dst, val.length);
        column(dst).put(val);
    }

    void putString(Column dst, String val) {
        putBytes(dst, val.getBytes(UTF_8));
    }

    void putUint64(Column dst, long val) {
        column(dst).putUint64(val);
    }

    void putSint64(Column dst, long val) {
        column(dst).putSint64(val);
    }

    void putFloat32(Column dst, float val) {
        column(dst).putFloat32(val);
    }

    void putFloat64(Column dst, double val) {
        column(dst).putFloat64(val);
    }

    int length() {
        int n = 0;
        for (int i = 0; i < buffers.length; i++) {
            if (Buffer.isEmpty(buffers[i])) {
                continue;
            }
            int len = ProtoUtil.fieldLen(i, buffers[i].length());
            if (len > Integer.MAX_VALUE - n) {
                throw new BufferOverflowException();
            }
            n += len;
        }
        return n;
    }

    void renderProtobufTo(ByteBuffer p) {
        for (int i = 0; i < buffers.length; i++) {
            if (!Buffer.isEmpty(buffers[i])) {
                p.putBytesField(i, buffers[i]);
            }
        }
    }
}
