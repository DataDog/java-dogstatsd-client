/* Unless explicitly stated otherwise all files in this repository are
 * licensed under the Apache 2.0 License.
 *
 * This product includes software developed at Datadog
 *  (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.
 */

package com.datadoghq.dogstatsd.http.forwarder;

import java.net.URI;

class Payload {
    final URI url;
    final byte[] bytes;

    Payload(URI url, byte[] bytes) {
        this.url = url;
        this.bytes = bytes;
    }
}
