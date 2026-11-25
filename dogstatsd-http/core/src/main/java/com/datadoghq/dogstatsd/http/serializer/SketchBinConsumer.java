/* Unless explicitly stated otherwise all files in this repository are
 * licensed under the Apache 2.0 License.
 *
 * This product includes software developed at Datadog
 *  (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.
 */

package com.datadoghq.dogstatsd.http.serializer;

import com.datadoghq.dogstatsd.Sketch;
import java.nio.BufferOverflowException;

/** Writes the bins of one sketch point into the sketch bin columns of a record. */
class SketchBinConsumer implements Sketch.BinConsumer {
    static final long maxBinCount = (1L << 32) - 1;
    static final long maxBinBytes = ProtoUtil.varintLen(maxBinCount);

    int numBins;
    final ColumnarBuffer r;
    final DeltaEncoder dk = new DeltaEncoder();

    SketchBinConsumer(ColumnarBuffer record) {
        r = record;
    }

    /**
     * Write the bins of one sketch point into the record.
     *
     * @throws BufferOverflowException if the bin data alone would exceed {@code maxPayloadSize}.
     *     Nothing is written in that case.
     */
    static void putBins(ColumnarBuffer r, int maxPayloadSize, Sketch sketch) {
        // Skip doing the work if just the bin data would exceed payload size limit.
        if (sketch.count() / maxBinCount * maxBinBytes >= maxPayloadSize) {
            throw new BufferOverflowException();
        }

        final SketchBinConsumer bc = new SketchBinConsumer(r);
        sketch.bins(bc);
        r.putUint64(Column.sketchNumBins, bc.numBins);
    }

    @Override
    public void consumeBin(short key, long count) {
        while (count > maxBinCount) {
            r.putSint64(Column.sketchBinKeys, dk.encode(key));
            r.putUint64(Column.sketchBinCnts, maxBinCount);
            count -= maxBinCount;
            numBins++;
        }
        r.putSint64(Column.sketchBinKeys, dk.encode(key));
        r.putUint64(Column.sketchBinCnts, count);
        numBins++;
    }
}
