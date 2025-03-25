package com.timgroup.statsd;

import jnr.constants.platform.Sock;
import jnr.unixsocket.UnixSocketAddress;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadFactory;
import jnr.unixsocket.UnixSocketAddress;

/**
 * Create a new StatsD client communicating with a StatsD instance on the specified host and port.
 * All messages send via this client will have their keys prefixed with the specified string. The
 * new client will attempt to open a connection to the StatsD server immediately upon instantiation,
 * and may throw an exception if that a connection cannot be established. Once a client has been
 * instantiated in this way, all exceptions thrown during subsequent usage are passed to the
 * specified handler and then consumed, guaranteeing that failures in metrics will not affect normal
 * code execution.
 */
public class NonBlockingStatsDClientBuilder implements Cloneable {

    /** The maximum number of bytes for a message that can be sent. */
    public int maxPacketSizeBytes = 0;

    public int port = NonBlockingStatsDClient.DEFAULT_DOGSTATSD_PORT;
    public int telemetryPort = NonBlockingStatsDClient.DEFAULT_DOGSTATSD_PORT;

    /** The maximum amount of unprocessed messages in the queue. */
    public int queueSize = NonBlockingStatsDClient.DEFAULT_QUEUE_SIZE;

    /** The timeout in milliseconds for blocking operations. Applies to unix sockets only. */
    public int timeout = NonBlockingStatsDClient.SOCKET_TIMEOUT_MS;

    /** The size for the network buffer pool. */
    public int bufferPoolSize = NonBlockingStatsDClient.DEFAULT_POOL_SIZE;

    /** The socket buffer size in bytes. Applies to unix sockets only. */
    public int socketBufferSize = NonBlockingStatsDClient.SOCKET_BUFFER_BYTES;

    /** The number of processor worker threads assembling buffers for submission. */
    public int processorWorkers = NonBlockingStatsDClient.DEFAULT_PROCESSOR_WORKERS;

    /** The number of sender worker threads submitting buffers to the socket. */
    public int senderWorkers = NonBlockingStatsDClient.DEFAULT_SENDER_WORKERS;

    /** Blocking or non-blocking implementation for statsd message queue. */
    public boolean blocking = NonBlockingStatsDClient.DEFAULT_BLOCKING;

    /** Enable sending client telemetry. */
    public boolean enableTelemetry = NonBlockingStatsDClient.DEFAULT_ENABLE_TELEMETRY;

    public boolean enableAggregation = NonBlockingStatsDClient.DEFAULT_ENABLE_AGGREGATION;

    /** Telemetry flush interval, in milliseconds. */
    public int telemetryFlushInterval = Telemetry.DEFAULT_FLUSH_INTERVAL;

    /** Aggregation flush interval, in milliseconds. 0 disables aggregation. */
    public int aggregationFlushInterval = StatsDAggregator.DEFAULT_FLUSH_INTERVAL;

    public int aggregationShards = StatsDAggregator.DEFAULT_SHARDS;

    /**
     * Enable/disable the client origin detection.
     *
     * <p>This feature requires Datadog Agent version &gt;=6.35.0 &amp;&amp; &lt;7.0.0 or Agent
     * versions &gt;=7.35.0. When enabled, the client tries to discover its container ID and sends
     * it to the Agent to enrich the metrics with container tags. Origin detection can be disabled
     * by configuring the environment variabe DD_ORIGIN_DETECTION_ENABLED=false The client tries to
     * read the container ID by parsing the file /proc/self/cgroup. This is not supported on
     * Windows.
     */
    public boolean originDetectionEnabled = NonBlockingStatsDClient.DEFAULT_ENABLE_ORIGIN_DETECTION;

    /**
     * The timeout in milliseconds for connecting to the StatsD server. Applies to unix sockets
     * only.
     *
     * <p>It is also used to detect if a connection is still alive and re-establish a new one if
     * needed.
     */
    public int connectionTimeout = NonBlockingStatsDClient.SOCKET_CONNECT_TIMEOUT_MS;

    /** Yields the IP address and socket of the StatsD server. */
    public Callable<SocketAddress> addressLookup;

    /** Yields the IP address and socket of the StatsD telemetry server destination. */
    public Callable<SocketAddress> telemetryAddressLookup;

    public String hostname;
    public String telemetryHostname;
    public String namedPipe;

    /** The prefix to apply to keys sent via this client. */
    public String prefix;

    /**
     * The entity id value used with an internal tag for tracking client entity.
     *
     * <p>If null the client default the value with the environment variable "DD_ENTITY_ID". If the
     * environment variable is not defined, the internal tag is not added.
     */
    public String entityID;

    /** Tags to be added to all content sent. */
    public String[] constantTags;

    /**
     * Allows passing the container ID, this will be used by the Agent to enrich metrics with
     * container tags.
     *
     * <p>This feature requires Datadog Agent version &gt;=6.35.0 &amp;&amp; &lt;7.0.0 or Agent
     * versions &gt;=7.35.0. When configured, the provided container ID is prioritized over the
     * container ID discovered via Origin Detection. When entityID or DD_ENTITY_ID are set, this
     * value is ignored.
     */
    public String containerID;

    /** Handler to use when an exception occurs during usage, may be null to indicate noop. */
    public StatsDClientErrorHandler errorHandler;

    public ThreadFactory threadFactory;
    public TagsCardinality tagsCardinality = null;

    public NonBlockingStatsDClientBuilder() {}

    public NonBlockingStatsDClientBuilder port(int val) {
        port = val;
        return this;
    }

    public NonBlockingStatsDClientBuilder telemetryPort(int val) {
        telemetryPort = val;
        return this;
    }

    /** The maximum amount of unprocessed messages in the queue. */
    public NonBlockingStatsDClientBuilder queueSize(int val) {
        queueSize = val;
        return this;
    }

    /** The timeout in milliseconds for blocking operations. Applies to unix sockets only. */
    public NonBlockingStatsDClientBuilder timeout(int val) {
        timeout = val;
        return this;
    }

    /**
     * The timeout in milliseconds for connecting to the StatsD server. Applies to unix sockets
     * only.
     *
     * <p>It is also used to detect if a connection is still alive and re-establish a new one if
     * needed.
     */
    public NonBlockingStatsDClientBuilder connectionTimeout(int val) {
        connectionTimeout = val;
        return this;
    }

    /** The size for the network buffer pool. */
    public NonBlockingStatsDClientBuilder bufferPoolSize(int val) {
        bufferPoolSize = val;
        return this;
    }

    /** The socket buffer size in bytes. Applies to unix sockets only. */
    public NonBlockingStatsDClientBuilder socketBufferSize(int val) {
        socketBufferSize = val;
        return this;
    }

    public NonBlockingStatsDClientBuilder maxPacketSizeBytes(int val) {
        maxPacketSizeBytes = val;
        return this;
    }

    /** The number of processor worker threads assembling buffers for submission. */
    public NonBlockingStatsDClientBuilder processorWorkers(int val) {
        processorWorkers = val;
        return this;
    }

    /** The number of sender worker threads submitting buffers to the socket. */
    public NonBlockingStatsDClientBuilder senderWorkers(int val) {
        senderWorkers = val;
        return this;
    }

    /** Blocking or non-blocking implementation for statsd message queue. */
    public NonBlockingStatsDClientBuilder blocking(boolean val) {
        blocking = val;
        return this;
    }

    /** Yields the IP address and socket of the StatsD server. */
    public NonBlockingStatsDClientBuilder addressLookup(Callable<SocketAddress> val) {
        addressLookup = val;
        return this;
    }

    /** Yields the IP address and socket of the StatsD telemetry server destination. */
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

    /** The prefix to apply to keys sent via this client. */
    public NonBlockingStatsDClientBuilder prefix(String val) {
        prefix = val;
        return this;
    }

    /**
     * The entity id value used with an internal tag for tracking client entity.
     *
     * <p>If null the client default the value with the environment variable "DD_ENTITY_ID". If the
     * environment variable is not defined, the internal tag is not added.
     */
    public NonBlockingStatsDClientBuilder entityID(String val) {
        entityID = val;
        return this;
    }

    /** Tags to be added to all content sent. */
    public NonBlockingStatsDClientBuilder constantTags(String... val) {
        constantTags = val;
        return this;
    }

    /** Handler to use when an exception occurs during usage, may be null to indicate noop. */
    public NonBlockingStatsDClientBuilder errorHandler(StatsDClientErrorHandler val) {
        errorHandler = val;
        return this;
    }

    /** Enable sending client telemetry. */
    public NonBlockingStatsDClientBuilder enableTelemetry(boolean val) {
        enableTelemetry = val;
        return this;
    }

    public NonBlockingStatsDClientBuilder enableAggregation(boolean val) {
        enableAggregation = val;
        return this;
    }

    /** Telemetry flush interval, in milliseconds. */
    public NonBlockingStatsDClientBuilder telemetryFlushInterval(int val) {
        telemetryFlushInterval = val;
        return this;
    }

    /** Aggregation flush interval, in milliseconds. 0 disables aggregation. */
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

    /**
     * Allows passing the container ID, this will be used by the Agent to enrich metrics with
     * container tags.
     *
     * <p>This feature requires Datadog Agent version &gt;=6.35.0 &amp;&amp; &lt;7.0.0 or Agent
     * versions &gt;=7.35.0. When configured, the provided container ID is prioritized over the
     * container ID discovered via Origin Detection. When entityID or DD_ENTITY_ID are set, this
     * value is ignored.
     */
    public NonBlockingStatsDClientBuilder containerID(String val) {
        containerID = val;
        return this;
    }

    /**
     * Enable/disable the client origin detection.
     *
     * <p>This feature requires Datadog Agent version &gt;=6.35.0 &amp;&amp; &lt;7.0.0 or Agent
     * versions &gt;7.35.0. When enabled, the client tries to discover its container ID and sends it
     * to the Agent to enrich the metrics with container tags. Origin detection can be disabled by
     * configuring the environment variabe DD_ORIGIN_DETECTION_ENABLED=false The client tries to
     * read the container ID by parsing the file /proc/self/cgroup. This is not supported on
     * Windows. The client prioritizes the value passed via or entityID or DD_ENTITY_ID (if set)
     * over the container ID.
     */
    public NonBlockingStatsDClientBuilder originDetectionEnabled(boolean val) {
        originDetectionEnabled = val;
        return this;
    }

    /**
     * Request that all metrics from this client to be enriched to specified tag cardinality.
     *
     * <p>See <a
     * href="https://docs.datadoghq.com/getting_started/tagging/assigning_tags/?tab=containerizedenvironments#tags-cardinality">Tags
     * cardinality documentation</a>.
     */
    public NonBlockingStatsDClientBuilder tagsCardinality(TagsCardinality cardinality) {
        tagsCardinality = cardinality;
        return this;
    }

    /**
     * NonBlockingStatsDClient factory method.
     *
     * @return the built NonBlockingStatsDClient.
     */
    public NonBlockingStatsDClient build() throws StatsDClientException {
        return new NonBlockingStatsDClient(resolve());
    }

    /**
     * {@link DirectStatsDClient} factory method.
     *
     * <p>It is an experimental extension of {@link StatsDClient} that allows for direct access to
     * some dogstatsd features. It is not recommended to use this client in production.
     *
     * @return the built DirectStatsDClient.
     * @see DirectStatsDClient
     */
    public DirectStatsDClient buildDirectStatsDClient() throws StatsDClientException {
        return new NonBlockingDirectStatsDClient(resolve());
    }

    /**
     * Creates a copy of this builder with any implicit elements resolved.
     *
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

        resolved.tagsCardinality = this.tagsCardinality;
        if (resolved.tagsCardinality == null) {
            resolved.tagsCardinality = TagsCardinality.fromString(System.getenv("DD_CARDINALITY"));
        }
        if (resolved.tagsCardinality == null) {
            resolved.tagsCardinality =
                    TagsCardinality.fromString(System.getenv("DATADOG_CARDINALITY"));
        }
        if (resolved.tagsCardinality == null) {
            resolved.tagsCardinality = TagsCardinality.DEFAULT;
        }

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
                    UnixSocketAddressWithTransport.TransportType.fromScheme(parsed.getScheme()));
        }

        return null;
    }

    /**
     * Create dynamic lookup for the given host name and port.
     *
     * @param hostname the host name of the targeted StatsD server.
     * @param port the port of the targeted StatsD server.
     * @return a function to perform the lookup
     */
    public static Callable<SocketAddress> volatileAddressResolution(
            final String hostname, final int port) {
        if (port == 0) {
            return new Callable<SocketAddress>() {
                @Override
                public SocketAddress call() throws UnknownHostException {
                    return new UnixSocketAddressWithTransport(
                            new UnixSocketAddress(hostname),
                            UnixSocketAddressWithTransport.TransportType.UDS);
                }
            };
        } else {
            return new Callable<SocketAddress>() {
                @Override
                public SocketAddress call() throws UnknownHostException {
                    return new InetSocketAddress(InetAddress.getByName(hostname), port);
                }
            };
        }
    }

    /**
     * Lookup the address for the given host name and cache the result.
     *
     * @param hostname the host name of the targeted StatsD server
     * @param port the port of the targeted StatsD server
     * @return a function that cached the result of the lookup
     * @throws Exception if the lookup fails, i.e. {@link UnknownHostException}
     */
    public static Callable<SocketAddress> staticAddressResolution(
            final String hostname, final int port) throws Exception {
        final SocketAddress address = volatileAddressResolution(hostname, port).call();
        return new Callable<SocketAddress>() {
            @Override
            public SocketAddress call() {
                return address;
            }
        };
    }

    protected static Callable<SocketAddress> staticNamedPipeResolution(String namedPipe) {
        final NamedPipeSocketAddress socketAddress = new NamedPipeSocketAddress(namedPipe);
        return new Callable<SocketAddress>() {
            @Override
            public SocketAddress call() {
                return socketAddress;
            }
        };
    }

    protected static Callable<SocketAddress> staticUnixResolution(
            final String path, final UnixSocketAddressWithTransport.TransportType transportType) {
        return new Callable<SocketAddress>() {
            @Override public SocketAddress call() {
                SocketAddress socketAddress;
                // Use native UDS support for compatible Java versions and jnr-unixsocket support otherwise.
                if (VersionUtils.isJavaVersionAtLeast(16)) {
                    try {
                        // Use reflection to avoid compiling Java 16+ classes in incompatible versions
                        Class<?> unixDomainSocketAddressClass = Class.forName("java.net.UnixDomainSocketAddress");
                        Method ofMethod = unixDomainSocketAddressClass.getMethod("of", String.class);
                        // return type SocketAddress instead of UnixSocketAddress for compatibility with the native SocketChannels in Unix*ClientChannel.java
                        socketAddress = (SocketAddress) ofMethod.invoke(null, path);
                    } catch (Exception e) {
                        throw new StatsDClientException("Failed to create UnixSocketAddress for native UDS implementation", e);
                    }
                } else {
                    socketAddress = new UnixSocketAddress(path);
                }
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
     * @throws StatsDClientException if the environment variable is not set
     */
    private static String getHostnameFromEnvVar() {
        final String hostname = System.getenv(NonBlockingStatsDClient.DD_AGENT_HOST_ENV_VAR);
        if (hostname == null) {
            throw new StatsDClientException(
                    "Failed to retrieve agent hostname from environment variable", null);
        }
        return hostname;
    }

    /**
     * Retrieves dogstatsd port from the environment variable "DD_DOGSTATSD_PORT".
     *
     * @return dogstatsd port from the environment variable "DD_DOGSTATSD_PORT"
     * @throws StatsDClientException if the environment variable is an integer
     */
    private static int getPortFromEnvVar(final int defaultPort) {
        final String statsDPortString =
                System.getenv(NonBlockingStatsDClient.DD_DOGSTATSD_PORT_ENV_VAR);
        if (statsDPortString == null) {
            return defaultPort;
        } else {
            try {
                final int statsDPort = Integer.parseInt(statsDPortString);
                return statsDPort;
            } catch (final NumberFormatException e) {
                throw new StatsDClientException(
                        "Failed to parse "
                                + NonBlockingStatsDClient.DD_DOGSTATSD_PORT_ENV_VAR
                                + "environment variable value",
                        e);
            }
        }
    }
}
