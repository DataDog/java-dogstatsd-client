package com.timgroup.statsd;

public class TestHelpers
{
    static boolean isLinux() {
        return System.getProperty("os.name").toLowerCase().contains("linux");
    }

    static boolean isMac() {
        return System.getProperty("os.name").toLowerCase().contains("mac");
    }

    // Check if jnr.unixsocket is on the classpath.
    static boolean isJnrAvailable() {
        try {
            Class.forName("jnr.unixsocket.UnixDatagramChannel");
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    static boolean isUdsAvailable() {
        return (isLinux() || isMac()) && isJnrAvailable();
    }
}
