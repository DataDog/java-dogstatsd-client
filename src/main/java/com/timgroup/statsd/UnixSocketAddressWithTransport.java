package com.timgroup.statsd;

import java.net.SocketAddress;
import java.util.Objects;

public class UnixSocketAddressWithTransport extends SocketAddress {

    private final SocketAddress address;
    private final TransportType transportType;

    public enum TransportType {
        UDS_STREAM("uds-stream"),
        UDS_DATAGRAM("uds-datagram"),
        UDS("uds");

        private final String transportType;

        TransportType(String transportType) {
            this.transportType = transportType;
        }

        String getTransportType() {
            return transportType;
        }

        static TransportType fromScheme(String scheme) {
            switch (scheme) {
                case "unixstream":
                    return UDS_STREAM;
                case "unixgram":
                    return UDS_DATAGRAM;
                case "unix":
                    return UDS;
                default:
                    break;
            }
            throw new IllegalArgumentException("Unknown scheme: " + scheme);
        }
    }

    public UnixSocketAddressWithTransport(final SocketAddress address, final TransportType transportType) {
        this.address = address;
        this.transportType = transportType;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        UnixSocketAddressWithTransport that = (UnixSocketAddressWithTransport) other;
        return Objects.equals(address, that.address) && transportType == that.transportType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(address, transportType);
    }

    SocketAddress getAddress() {
        return address;
    }

    TransportType getTransportType() {
        return transportType;
    }
}
