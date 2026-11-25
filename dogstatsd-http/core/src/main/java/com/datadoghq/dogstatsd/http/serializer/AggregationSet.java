/* Unless explicitly stated otherwise all files in this repository are
 * licensed under the Apache 2.0 License.
 *
 * This product includes software developed at Datadog
 *  (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.
 */

package com.datadoghq.dogstatsd.http.serializer;

import static java.nio.charset.StandardCharsets.UTF_8;

/** Builder for an aggregation set metric, carrying the distinct members seen during the window. */
public class AggregationSet extends Metric<AggregationSet> {
    private byte[] lastMember;
    private long numMembers;

    AggregationSet(AggregationPayloadBuilder pb, String name) {
        super(pb, MetricType.SET, name);
    }

    @Override
    AggregationSet self() {
        return this;
    }

    /**
     * Add a member to the set.
     *
     * <p>Members must be added in strictly ascending shortlex order: shorter UTF-8 encoding first,
     * then bytewise for encodings of the same length.
     *
     * @param member Member of the set.
     * @return This.
     * @throws IllegalArgumentException if {@code member} is not greater than the previous member.
     *     The metric is left unchanged and remains valid.
     */
    public AggregationSet addMember(String member) {
        byte[] bytes = member.getBytes(UTF_8);
        if (lastMember != null && compareShortlex(bytes, lastMember) <= 0) {
            throw new IllegalArgumentException(
                    "set members must be distinct and added in shortlex order");
        }
        pb.currentRecord().putBytes(Column.setStrs, bytes);
        lastMember = bytes;
        numMembers++;
        return this;
    }

    /** Compare by length, then bytewise as unsigned values. */
    static int compareShortlex(byte[] a, byte[] b) {
        if (a.length != b.length) {
            return a.length < b.length ? -1 : 1;
        }
        for (int i = 0; i < a.length; i++) {
            int x = a[i] & 0xff;
            int y = b[i] & 0xff;
            if (x != y) {
                return x < y ? -1 : 1;
            }
        }
        return 0;
    }

    @Override
    boolean hasData() {
        return numMembers > 0;
    }

    @Override
    ValueType valueType() {
        return ValueType.zero;
    }

    @Override
    void encodeValues(ValueType valueType) {
        pb.currentRecord().putUint64(Column.valsUint64, numMembers);
    }
}
