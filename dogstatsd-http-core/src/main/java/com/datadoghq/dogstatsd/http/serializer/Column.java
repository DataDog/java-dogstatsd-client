/* Unless explicitly stated otherwise all files in this repository are
 * licensed under the Apache 2.0 License.
 *
 * This product includes software developed at Datadog
 *  (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.
 */

package com.datadoghq.dogstatsd.http.serializer;

enum Column {
    dictNameStr(1),
    dictTagStr(2),
    dictTagsets(3),
    dictResourceStr(4),
    dictResourceLen(5),
    dictResourceType(6),
    dictResourceName(7),
    dictSourceTypeName(8),
    dictOriginInfo(9),
    types(10),
    nameRefs(11),
    tagsetRefs(12),
    resourcesRefs(13),
    intervals(14),
    numPoints(15),
    timestamps(16),
    valsSint64(17),
    valsFloat32(18),
    valsFloat64(19),
    sketchNumBins(20),
    sketchBinKeys(21),
    sketchBinCnts(22),
    sourceTypeNameRefs(23),
    originInfoRefs(24);

    static final int MAX = 24;

    final int id;

    Column(int id) {
        this.id = id;
    }
}
