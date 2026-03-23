/* Unless explicitly stated otherwise all files in this repository are
 * licensed under the Apache 2.0 License.
 *
 * This product includes software developed at Datadog
 *  (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.
 */

package com.datadoghq.dogstatsd.http.serializer;

import static org.junit.Assert.fail;

import java.util.Formatter;

class TestUtil {
    static void hexdump(String prefix, Formatter out, byte[] p, int base, int len) {
        for (int i = 0; i < len; i++) {
            if (i % 16 == 0) {
                out.format("%s", prefix);
            }
            if (base + i >= p.length) {
                out.format("unexpected end of field\n");
                return;
            }
            out.format(" %02x", p[base + i]);
            if ((i + 1) % 16 == 0 || i == len - 1) {
                for (int j = i % 16; j < 16; j++) {
                    out.format("   ");
                }
                out.format("    ");
                for (int j = i - i % 16; j <= i; j++) {
                    if (p[base + j] >= 32 && p[base + j] < 127) {
                        out.format("%c", p[base + j]);
                    } else {
                        out.format(".");
                    }
                }
                out.format("\n");
            }
        }
    }

    static class Varint {
        int len;
        int val;

        void read(byte[] p, int base) {
            len = 0;
            val = 0;
            for (int sw = 0; base + len < p.length; sw += 7) {
                int b = p[base + len++];
                if (b < 0) b += 256;
                val |= (b & 127) << sw;
                if ((b & 128) == 0) {
                    break;
                }
            }
        }
    }

    static final String indent[] = new String[] {"", "  "};

    static void protodump(Formatter out, byte[] p, int base, int len, int depth) {
        Varint var = new Varint();

        for (int idx = 0; idx < len; ) {
            int start = idx;
            var.read(p, base + idx);
            int id = var.val >> 3;
            int ty = var.val & 3;
            if (var.len == 0) {
                out.format("unexpected end of message\n");
                return;
            }
            idx += var.len;
            String prefix = String.format("%s(%2d) ", indent[depth], id);
            if (ty != ProtoUtil.TYPE_BYTES) {
                out.format("%sunexpected data type %x at offset %d\n", prefix, ty, base + idx);
                hexdump(prefix, out, p, base + start, idx - start);
                return;
            }

            var.read(p, base + idx);
            idx += var.len;
            int flen = var.val;
            if (var.len == 0) {
                out.format("unexpected end of message\n");
                return;
            }
            if (depth == 0) {
                hexdump(prefix, out, p, base + start, idx - start);
                protodump(out, p, base + idx, flen, depth + 1);
            } else {
                hexdump(prefix, out, p, base + start, flen + idx - start);
            }
            idx += flen;
        }
    }

    static String protodump(byte[] p) {
        Formatter out = new Formatter();
        protodump(out, p, 0, p.length, 0);
        return out.out().toString();
    }

    static String protodump(int[] p) {
        byte[] pb = new byte[p.length];
        for (int i = 0; i < p.length; i++) pb[i] = (byte) p[i];
        return protodump(pb);
    }

    static void formatTwoCols(Formatter out, String hl, String hr, String dl, String dr) {
        final String fmt = "%-80s%3s%-80s\n";
        out.format(fmt, hl, "", hr);
        String[] linesl = dl.split("\n");
        String[] linesr = dr.split("\n");
        int i = 0;
        for (; i < linesl.length && i < linesr.length; i++) {
            String l = linesl[i];
            String r = linesr[i];
            out.format(fmt, l, l.equals(r) ? "" : "≠ ", r);
        }
        for (; i < linesl.length; i++) {
            out.format(fmt, linesl[i], "", "");
        }
        for (; i < linesr.length; i++) {
            out.format(fmt, "", "", linesr[i]);
        }
    }

    // expected is int[] to be able to write unsigned byte values wtihout conversion.
    static void assertPayload(byte[] got, int[] expected) {
        boolean same = got.length == expected.length;
        for (int i = 0; same && i < got.length; i++) {
            same &= got[i] == (byte) expected[i];
        }
        if (!same) {
            Formatter out = new Formatter();
            out.format("payloads do not match:\n");
            formatTwoCols(out, "GOT", "EXPECTED", protodump(got), protodump(expected));
            out.format("(field id in parens, fields printed including header and length)");
            fail(out.out().toString());
        }
    }
}
