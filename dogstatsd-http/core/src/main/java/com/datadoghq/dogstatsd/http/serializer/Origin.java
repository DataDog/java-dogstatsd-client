/* Unless explicitly stated otherwise all files in this repository are
 * licensed under the Apache 2.0 License.
 *
 * This product includes software developed at Datadog
 *  (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.
 */

package com.datadoghq.dogstatsd.http.serializer;

enum Origin {
    undefined(0, 0, 0),
    dogstatsd(10, 10, 0);

    int product;
    int category;
    int service;

    Origin(int product, int category, int service) {
        this.product = product;
        this.category = category;
        this.service = service;
    }
}
