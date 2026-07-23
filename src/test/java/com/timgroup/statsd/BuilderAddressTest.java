package com.timgroup.statsd;

import static org.junit.Assert.assertEquals;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import jnr.unixsocket.UnixSocketAddress;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class BuilderAddressTest {

    // Controlled environment so address resolution only sees the variables this test sets.
    Map<String, String> env;

    final String url;
    final String host;
    final String port;
    final String pipe;
    final SocketAddress expected;

    public BuilderAddressTest(
            String url, String host, String port, String pipe, SocketAddress expected) {
        this.url = url;
        this.host = host;
        this.port = port;
        this.pipe = pipe;
        this.expected = expected;
    }

    private static final int defaultPort = NonBlockingStatsDClient.DEFAULT_DOGSTATSD_PORT;

    @Parameters
    public static Collection<Object[]> parameters() {
        ArrayList params = new ArrayList();

        params.addAll(
                Arrays.asList(
                        new Object[][] {
                            // DD_DOGSTATSD_URL
                            {
                                "udp://1.1.1.1",
                                null,
                                null,
                                null,
                                new InetSocketAddress("1.1.1.1", defaultPort)
                            },
                            {
                                "udp://1.1.1.1:9999",
                                null,
                                null,
                                null,
                                new InetSocketAddress("1.1.1.1", 9999)
                            },
                            {
                                "\\\\.\\pipe\\foo",
                                null,
                                null,
                                null,
                                new NamedPipeSocketAddress("\\\\.\\pipe\\foo")
                            },

                            // DD_AGENT_HOST
                            {
                                null,
                                "1.1.1.1",
                                null,
                                null,
                                new InetSocketAddress("1.1.1.1", defaultPort)
                            },

                            // DD_AGENT_HOST, DD_DOGSTATSD_PORT
                            {null, "1.1.1.1", "9999", null, new InetSocketAddress("1.1.1.1", 9999)},
                            {
                                null,
                                null,
                                null,
                                "foo",
                                new NamedPipeSocketAddress("\\\\.\\pipe\\foo")
                            },

                            // DD_DOGSTATSD_URL overrides other env vars.
                            {
                                "udp://1.1.1.1",
                                null,
                                null,
                                "foo",
                                new InetSocketAddress("1.1.1.1", defaultPort)
                            },
                            {
                                "udp://1.1.1.1:9999",
                                null,
                                null,
                                "foo",
                                new InetSocketAddress("1.1.1.1", 9999)
                            },
                            {
                                "\\\\.\\pipe\\foo",
                                null,
                                null,
                                "bar",
                                new NamedPipeSocketAddress("\\\\.\\pipe\\foo")
                            },
                            {
                                "\\\\.\\pipe\\foo",
                                "1.1.1.1",
                                null,
                                null,
                                new NamedPipeSocketAddress("\\\\.\\pipe\\foo")
                            },
                            {
                                "\\\\.\\pipe\\foo",
                                "1.1.1.1",
                                "9999",
                                null,
                                new NamedPipeSocketAddress("\\\\.\\pipe\\foo")
                            },

                            // DD_DOGSTATSD_NAMED_PIPE overrides DD_AGENT_HOST.
                            {
                                null,
                                "1.1.1.1",
                                null,
                                "foo",
                                new NamedPipeSocketAddress("\\\\.\\pipe\\foo")
                            },
                            {
                                null,
                                "1.1.1.1",
                                "9999",
                                "foo",
                                new NamedPipeSocketAddress("\\\\.\\pipe\\foo")
                            },
                        }));

        if (TestHelpers.isJnrAvailable()) {
            // Here we use FakeUnixSocketAddress instead of UnixSocketAddress to make sure we can
            // always run the tests without jnr-unixsock.

            UnixSocketAddressWithTransport unixDsd =
                    new UnixSocketAddressWithTransport(
                            new FakeUnixSocketAddress("/dsd.sock"),
                            UnixSocketAddressWithTransport.TransportType.UDS);
            UnixSocketAddressWithTransport unixDgramDsd =
                    new UnixSocketAddressWithTransport(
                            new FakeUnixSocketAddress("/dsd.sock"),
                            UnixSocketAddressWithTransport.TransportType.UDS_DATAGRAM);
            UnixSocketAddressWithTransport unixStreamDsd =
                    new UnixSocketAddressWithTransport(
                            new FakeUnixSocketAddress("/dsd.sock"),
                            UnixSocketAddressWithTransport.TransportType.UDS_STREAM);

            params.addAll(
                    Arrays.asList(
                            new Object[][] {
                                {"unix:///dsd.sock", null, null, null, unixDsd},
                                {"unix://unused/dsd.sock", null, null, null, unixDsd},
                                {"unix://unused:9999/dsd.sock", null, null, null, unixDsd},
                                {null, "/dsd.sock", "0", null, unixDsd},
                                {"unix:///dsd.sock", "1.1.1.1", "9999", null, unixDsd},
                                {"unixgram:///dsd.sock", null, null, null, unixDgramDsd},
                                {"unixstream:///dsd.sock", null, null, null, unixStreamDsd},
                            }));
        }

        return params;
    }

    static class FakeUnixSocketAddress extends SocketAddress {
        final String path;

        public FakeUnixSocketAddress(String path) {
            this.path = path;
        }

        public String getPath() {
            return path;
        }
    }

    @Before
    public void set() {
        env = new HashMap<>();
        set(NonBlockingStatsDClient.DD_DOGSTATSD_URL_ENV_VAR, url);
        set(NonBlockingStatsDClient.DD_AGENT_HOST_ENV_VAR, host);
        set(NonBlockingStatsDClient.DD_DOGSTATSD_PORT_ENV_VAR, port);
        set(NonBlockingStatsDClient.DD_NAMED_PIPE_ENV_VAR, pipe);
    }

    void set(String name, String val) {
        if (val != null) {
            env.put(name, val);
        }
    }

    @Test(timeout = 5000L)
    public void address_resolution() throws Exception {
        NonBlockingStatsDClientBuilder b;

        // Default configuration matches env vars
        b = new NonBlockingStatsDClientBuilder().withEnvironmentVariables(env).resolve();
        SocketAddress actual = b.addressLookup.call();

        // Make it possible to run this code even if we don't have jnr-unixsocket.
        if (expected instanceof UnixSocketAddressWithTransport) {
            UnixSocketAddressWithTransport a = (UnixSocketAddressWithTransport) actual;
            UnixSocketAddressWithTransport e = (UnixSocketAddressWithTransport) expected;
            assertEquals(
                    ((FakeUnixSocketAddress) e.getAddress()).getPath(),
                    ((UnixSocketAddress) a.getAddress()).path());
            assertEquals(e.getTransportType(), a.getTransportType());
        } else {
            assertEquals(expected, actual);
        }

        // Explicit configuration is used regardless of environment variables.
        b =
                new NonBlockingStatsDClientBuilder()
                        .withEnvironmentVariables(env)
                        .hostname("2.2.2.2")
                        .resolve();
        assertEquals(new InetSocketAddress("2.2.2.2", defaultPort), b.addressLookup.call());

        b =
                new NonBlockingStatsDClientBuilder()
                        .withEnvironmentVariables(env)
                        .hostname("2.2.2.2")
                        .port(2222)
                        .resolve();
        assertEquals(new InetSocketAddress("2.2.2.2", 2222), b.addressLookup.call());

        b =
                new NonBlockingStatsDClientBuilder()
                        .withEnvironmentVariables(env)
                        .namedPipe("ook")
                        .resolve();
        assertEquals(new NamedPipeSocketAddress("ook"), b.addressLookup.call());
    }
}
