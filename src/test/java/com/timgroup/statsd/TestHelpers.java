package com.timgroup.statsd;

import java.util.ArrayList;

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

    static Object[][] permutations(Object[][] in) {
        ArrayList<Object[]> out = new ArrayList<>();
        permutations(out, in, 0, new Object[in.length]);
        return out.toArray(new Object[][]{});
    }

    static void permutations(ArrayList<Object[]> out, Object[][] in, int index, Object[] buf) {
        if (index >= in.length) {
            out.add(buf.clone());
            return;
        }
        for (Object obj : in[index]) {
            buf[index] = obj;
            permutations(out, in, index + 1, buf);
        }
    }
}
