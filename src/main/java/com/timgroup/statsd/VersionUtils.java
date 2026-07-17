package com.timgroup.statsd;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.ProtocolFamily;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;

// Logic copied from dd-trace-java Platform class. See:
// https://github.com/DataDog/dd-trace-java/blob/master/internal-api/src/main/java/datadog/trace/api/Platform.java
final class VersionUtils {
    private static final Version JAVA_VERSION =
            parseJavaVersion(System.getProperty("java.version"));

    private VersionUtils() {}

    static Version parseJavaVersion(String javaVersion) {
        // Remove pre-release part, usually -ea
        final int indexOfDash = javaVersion.indexOf('-');
        if (indexOfDash >= 0) {
            javaVersion = javaVersion.substring(0, indexOfDash);
        }

        int major = 0;
        int minor = 0;
        int update = 0;

        try {
            List<Integer> nums = splitDigits(javaVersion);
            major = nums.get(0);

            // for java 1.6/1.7/1.8
            if (major == 1) {
                major = nums.get(1);
                minor = nums.size() > 2 ? nums.get(2) : 0;
                update = nums.size() > 3 ? nums.get(3) : 0;
            } else {
                minor = nums.size() > 1 ? nums.get(1) : 0;
                update = nums.size() > 2 ? nums.get(2) : 0;
            }
        } catch (NumberFormatException | IndexOutOfBoundsException e) {
            // unable to parse version string - do nothing
        }
        return new Version(major, minor, update);
    }

    private static List<Integer> splitDigits(String str) {
        List<Integer> results = new ArrayList<>();

        int len = str.length();

        int value = 0;
        for (int i = 0; i < len; i++) {
            char ch = str.charAt(i);
            if (ch >= '0' && ch <= '9') {
                value = value * 10 + (ch - '0');
            } else if (ch == '.' || ch == '_' || ch == '+') {
                results.add(value);
                value = 0;
            } else {
                throw new NumberFormatException();
            }
        }
        results.add(value);
        return results;
    }

    static final class Version {
        final int major;
        final int minor;
        final int update;

        Version(int major, int minor, int update) {
            this.major = major;
            this.minor = minor;
            this.update = update;
        }

        boolean isAtLeast(int major, int minor, int update) {
            return isAtLeast(this.major, this.minor, this.update, major, minor, update);
        }

        private static boolean isAtLeast(
                int major,
                int minor,
                int update,
                int atLeastMajor,
                int atLeastMinor,
                int atLeastUpdate) {
            return (major > atLeastMajor)
                    || (major == atLeastMajor && minor > atLeastMinor)
                    || (major == atLeastMajor && minor == atLeastMinor && update >= atLeastUpdate);
        }
    }

    static boolean isJavaVersionAtLeast(int major) {
        return JAVA_VERSION.isAtLeast(major, 0, 0);
    }

    /**
     * Opens a {@link SocketChannel} for Unix domain sockets using {@code
     * StandardProtocolFamily.UNIX}, available since Java 16. Uses reflection to avoid a
     * compile-time dependency on Java 16+ classes.
     */
    @SuppressWarnings("unchecked")
    static SocketChannel openUnixSocketChannel() throws IOException {
        try {
            Class<?> standardProtocolFamilyClass = Class.forName("java.net.StandardProtocolFamily");
            Object unixProtocol = Enum.valueOf((Class<Enum>) standardProtocolFamilyClass, "UNIX");
            Method openMethod = SocketChannel.class.getMethod("open", ProtocolFamily.class);
            return (SocketChannel) openMethod.invoke(null, unixProtocol);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException) {
                throw (IOException) cause;
            }
            throw new IOException("Failed to open Unix domain SocketChannel", e);
        } catch (Exception e) {
            throw new IOException("Failed to open Unix domain SocketChannel", e);
        }
    }

    /**
     * Creates a {@code java.net.UnixDomainSocketAddress} for the given path using reflection.
     * Available since Java 16.
     */
    static SocketAddress newUnixDomainSocketAddress(String path) {
        try {
            Class<?> cls = Class.forName("java.net.UnixDomainSocketAddress");
            Method of = cls.getMethod("of", String.class);
            return (SocketAddress) of.invoke(null, path);
        } catch (InvocationTargetException e) {
            throw new StatsDClientException(
                    "Failed to create UnixDomainSocketAddress for path: " + path, e.getCause());
        } catch (Exception e) {
            throw new StatsDClientException(
                    "Failed to create UnixDomainSocketAddress for path: " + path, e);
        }
    }
}
