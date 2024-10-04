package com.timgroup.statsd;

import jnr.constants.platform.Sock;
import jnr.unixsocket.UnixSocketAddress;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadFactory;

public class NonBlockingStatsDClientBuilder implements Cloneable {

    /**
     * 1400 chosen as default here so that the number of bytes in a message plus the number of bytes required
     * for additional udp headers should be under the 1500 Maximum Transmission Unit for ethernet.
     * See https://github.com/DataDog/java-dogstatsd-client/pull/17 for discussion.
     */

    public int maxPacketSizeBytes = 0;
    public int port = NonBlockingStatsDClient.DEFAULT_DOGSTATSD_PORT;
    public int telemetryPort = NonBlockingStatsDClient.DEFAULT_DOGSTATSD_PORT;
    public int queueSize = NonBlockingStatsDClient.DEFAULT_QUEUE_SIZE;
    public int timeout = NonBlockingStatsDClient.SOCKET_TIMEOUT_MS;
    public int bufferPoolSize = NonBlockingStatsDClient.DEFAULT_POOL_SIZE;
    public int socketBufferSize = NonBlockingStatsDClient.SOCKET_BUFFER_BYTES;
    public int processorWorkers = NonBlockingStatsDClient.DEFAULT_PROCESSOR_WORKERS;
    public int senderWorkers = NonBlockingStatsDClient.DEFAULT_SENDER_WORKERS;
    public boolean blocking = NonBlockingStatsDClient.DEFAULT_BLOCKING;
    public boolean enableTelemetry = NonBlockingStatsDClient.DEFAULT_ENABLE_TELEMETRY;
    public boolean enableAggregation = NonBlockingStatsDClient.DEFAULT_ENABLE_AGGREGATION;
    public int telemetryFlushInterval = Telemetry.DEFAULT_FLUSH_INTERVAL;
    public int aggregationFlushInterval = StatsDAggregator.DEFAULT_FLUSH_INTERVAL;
    public int aggregationShards = StatsDAggregator.DEFAULT_SHARDS;
    public boolean originDetectionEnabled = NonBlockingStatsDClient.DEFAULT_ENABLE_ORIGIN_DETECTION;
    public int connectionTimeout = NonBlockingStatsDClient.SOCKET_CONNECT_TIMEOUT_MS;

    public Callable<SocketAddress> addressLookup;
    public Callable<SocketAddress> telemetryAddressLookup;

    public String hostname;
    public String telemetryHostname;
    public String namedPipe;
    public String prefix;
    public String entityID;
    public String[] constantTags;
    public String containerID;

    public StatsDClientErrorHandler errorHandler;
    public ThreadFactory threadFactory;

    public NonBlockingStatsDClientBuilder() { }

    public NonBlockingStatsDClientBuilder port(int val) {
        port = val;
        return this;
    }

    public NonBlockingStatsDClientBuilder telemetryPort(int val) {
        telemetryPort = val;
        return this;
    }

    public NonBlockingStatsDClientBuilder queueSize(int val) {
        queueSize = val;
        return this;
    }

    public NonBlockingStatsDClientBuilder timeout(int val) {
        timeout = val;
        return this;
    }

    public NonBlockingStatsDClientBuilder connectionTimeout(int val) {
        connectionTimeout = val;
        return this;
    }

    public NonBlockingStatsDClientBuilder bufferPoolSize(int val) {
        bufferPoolSize = val;
        return this;
    }

    public NonBlockingStatsDClientBuilder socketBufferSize(int val) {
        socketBufferSize = val;
        return this;
    }

    public NonBlockingStatsDClientBuilder maxPacketSizeBytes(int val) {
        maxPacketSizeBytes = val;
        return this;
    }

    public NonBlockingStatsDClientBuilder processorWorkers(int val) {
        processorWorkers = val;
        return this;
    }

    public NonBlockingStatsDClientBuilder senderWorkers(int val) {
        senderWorkers = val;
        return this;
    }

    public NonBlockingStatsDClientBuilder blocking(boolean val) {
        blocking = val;
        return this;
    }

    public NonBlockingStatsDClientBuilder addressLookup(Callable<SocketAddress> val) {
        addressLookup = val;
        return this;
    }

    public NonBlockingStatsDClientBuilder telemetryAddressLookup(Callable<SocketAddress> val) {
        telemetryAddressLookup = val;
        return this;
    }

    public NonBlockingStatsDClientBuilder hostname(String val) {
        hostname = val;
        return this;
    }

    public NonBlockingStatsDClientBuilder telemetryHostname(String val) {
        telemetryHostname = val;
        return this;
    }

    public NonBlockingStatsDClientBuilder namedPipe(String val) {
        namedPipe = val;
        return this;
    }

    public NonBlockingStatsDClientBuilder address(String address) {
        addressLookup = getAddressLookupFromUrl(address);
        return this;
    }

    public NonBlockingStatsDClientBuilder telemetryAddress(String address) {
        telemetryAddressLookup = getAddressLookupFromUrl(address);
        return this;
    }

    public NonBlockingStatsDClientBuilder prefix(String val) {
        prefix = val;
        return this;
    }

    public NonBlockingStatsDClientBuilder entityID(String val) {
        entityID = val;
        return this;
    }

    public NonBlockingStatsDClientBuilder constantTags(String... val) {
        constantTags = val;
        return this;
    }

    public NonBlockingStatsDClientBuilder errorHandler(StatsDClientErrorHandler val) {
        errorHandler = val;
        return this;
    }

    public NonBlockingStatsDClientBuilder enableTelemetry(boolean val) {
        enableTelemetry = val;
        return this;
    }

    public NonBlockingStatsDClientBuilder enableAggregation(boolean val) {
        enableAggregation = val;
        return this;
    }

    public NonBlockingStatsDClientBuilder telemetryFlushInterval(int val) {
        telemetryFlushInterval = val;
        return this;
    }

    public NonBlockingStatsDClientBuilder aggregationFlushInterval(int val) {
        aggregationFlushInterval = val;
        return this;
    }

    public NonBlockingStatsDClientBuilder aggregationShards(int val) {
        aggregationShards = val;
        return this;
    }

    public NonBlockingStatsDClientBuilder threadFactory(ThreadFactory val) {
        threadFactory = val;
        return this;
    }

    public NonBlockingStatsDClientBuilder containerID(String val) {
        containerID = val;
        return this;
    }

    public NonBlockingStatsDClientBuilder originDetectionEnabled(boolean val) {
        originDetectionEnabled = val;
        return this;
    }

    /**
     * NonBlockingStatsDClient factory method.
     * @return the built NonBlockingStatsDClient.
     */
    public NonBlockingStatsDClient build() throws StatsDClientException {
        return new NonBlockingStatsDClient(resolve());
    }

    /**
     * {@link DirectStatsDClient} factory method.
     *
     * <p>It is an experimental extension of {@link StatsDClient} that allows for direct access to some dogstatsd features.
     * It is not recommended to use this client in production.
     * @return the built DirectStatsDClient.
     * @see DirectStatsDClient
     */
    public DirectStatsDClient buildDirectStatsDClient() throws StatsDClientException {
        return new NonBlockingDirectStatsDClient(resolve());
    }

    /**
     * Creates a copy of this builder with any implicit elements resolved.
     * @return the resolved copy of the builder.
     */
    protected NonBlockingStatsDClientBuilder resolve() {
        NonBlockingStatsDClientBuilder resolved;

        try {
            resolved = (NonBlockingStatsDClientBuilder) clone();
        } catch (CloneNotSupportedException e) {
            throw new UnsupportedOperationException("clone");
        }

        Callable<SocketAddress> lookup = getAddressLookup();

        Callable<SocketAddress> telemetryLookup = telemetryAddressLookup;
        if (telemetryLookup == null) {
            if (telemetryHostname == null) {
                telemetryLookup = lookup;
            } else {
                telemetryLookup = staticAddress(telemetryHostname, telemetryPort);
            }
        }

        resolved.addressLookup = lookup;
        resolved.telemetryAddressLookup = telemetryLookup;

        return resolved;
    }

    private Callable<SocketAddress> getAddressLookup() {
        // First, use explicit configuration on the builder.
        if (addressLookup != null) {
            return addressLookup;
        }

        if (namedPipe != null) {
            return staticNamedPipeResolution(namedPipe);
        }

        if (hostname != null) {
            return staticAddress(hostname, port);
        }

        // Next, try various environment variables.
        String url = System.getenv(NonBlockingStatsDClient.DD_DOGSTATSD_URL_ENV_VAR);
        if (url != null) {
            return getAddressLookupFromUrl(url);
        }

        String namedPipeFromEnv = System.getenv(NonBlockingStatsDClient.DD_NAMED_PIPE_ENV_VAR);
        if (namedPipeFromEnv != null) {
            return staticNamedPipeResolution(namedPipeFromEnv);
        }

        String hostFromEnv = getHostnameFromEnvVar();
        int portFromEnv = getPortFromEnvVar(port);

        return staticAddress(hostFromEnv, portFromEnv);
    }

    private Callable<SocketAddress> getAddressLookupFromUrl(String url) {
        if (NamedPipeSocketAddress.isNamedPipe(url)) {
            return staticNamedPipeResolution(url);
        }

        URI parsed;
        try {
            parsed = new URI(url);
        } catch (Exception e) {
            return null;
        }

        if (parsed.getScheme().equals("udp")) {
            String uriHost = parsed.getHost();
            int uriPort = parsed.getPort();
            if (uriPort < 0) {
                uriPort = port;
            }
            return staticAddress(uriHost, uriPort);
        }

        if (parsed.getScheme().startsWith("unix")) {
            String uriPath = parsed.getPath();
            return staticUnixResolution(
                    uriPath,
                    UnixSocketAddressWithTransport.TransportType.fromScheme(parsed.getScheme())
            );
        }

        return null;
    }

    /**
     * Create dynamic lookup for the given host name and port.
     *
     * @param hostname
     *     the host name of the targeted StatsD server.
     * @param port
     *     the port of the targeted StatsD server.
     * @return a function to perform the lookup
     */
    public static Callable<SocketAddress> volatileAddressResolution(final String hostname, final int port) {
        if (port == 0) {
            return new Callable<SocketAddress>() {
                @Override public SocketAddress call() throws UnknownHostException {
                    return new UnixSocketAddressWithTransport(
                            new UnixSocketAddress(hostname),
                            UnixSocketAddressWithTransport.TransportType.UDS
                    );
                }
            };
        } else {
            return new Callable<SocketAddress>() {
                @Override public SocketAddress call() throws UnknownHostException {
                    return new InetSocketAddress(InetAddress.getByName(hostname), port);
                }
            };
        }
    }

    /**
     * Lookup the address for the given host name and cache the result.
     *
     * @param hostname the host name of the targeted StatsD server
     * @param port     the port of the targeted StatsD server
     * @return a function that cached the result of the lookup
     * @throws Exception if the lookup fails, i.e. {@link UnknownHostException}
     */
    public static Callable<SocketAddress> staticAddressResolution(final String hostname, final int port)
            throws Exception {
        final SocketAddress address = volatileAddressResolution(hostname, port).call();
        return new Callable<SocketAddress>() {
            @Override public SocketAddress call() {
                return address;
            }
        };
    }

    protected static Callable<SocketAddress> staticNamedPipeResolution(String namedPipe) {
        final NamedPipeSocketAddress socketAddress = new NamedPipeSocketAddress(namedPipe);
        return new Callable<SocketAddress>() {
            @Override public SocketAddress call() {
                return socketAddress;
            }
        };
    }

    protected static Callable<SocketAddress> staticUnixResolution(
            final String path,
            final UnixSocketAddressWithTransport.TransportType transportType) {
        return new Callable<SocketAddress>() {
            @Override public SocketAddress call() {
                final UnixSocketAddress socketAddress = new UnixSocketAddress(path);
                return new UnixSocketAddressWithTransport(socketAddress, transportType);
            }
        };
    }

    private static Callable<SocketAddress> staticAddress(final String hostname, final int port) {
        try {
            return staticAddressResolution(hostname, port);
        } catch (Exception e) {
            throw new StatsDClientException("Failed to lookup StatsD host", e);
        }
    }

    /**
     * Retrieves host name from the environment variable "DD_AGENT_HOST".
     *
     * @return host name from the environment variable "DD_AGENT_HOST"
     *
     * @throws StatsDClientException if the environment variable is not set
     */
    private static String getHostnameFromEnvVar() {
        final String hostname = System.getenv(NonBlockingStatsDClient.DD_AGENT_HOST_ENV_VAR);
        if (hostname == null) {
            throw new StatsDClientException("Failed to retrieve agent hostname from environment variable", null);
        }
        return hostname;
    }

    /**
     * Retrieves dogstatsd port from the environment variable "DD_DOGSTATSD_PORT".
     *
     * @return dogstatsd port from the environment variable "DD_DOGSTATSD_PORT"
     *
     * @throws StatsDClientException if the environment variable is an integer
     */
    private static int getPortFromEnvVar(final int defaultPort) {
        final String statsDPortString = System.getenv(NonBlockingStatsDClient.DD_DOGSTATSD_PORT_ENV_VAR);
        if (statsDPortString == null) {
            return defaultPort;
        } else {
            try {
                final int statsDPort = Integer.parseInt(statsDPortString);
                return statsDPort;
            } catch (final NumberFormatException e) {
                throw new StatsDClientException("Failed to parse "
                        + NonBlockingStatsDClient.DD_DOGSTATSD_PORT_ENV_VAR + "environment variable value", e);
            }
        }
    }


}

