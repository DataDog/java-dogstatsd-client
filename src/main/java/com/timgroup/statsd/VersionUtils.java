package com.timgroup.statsd;

import java.util.ArrayList;
import java.util.List;

// Logic copied from dd-trace-java Platform class. See:
// https://github.com/DataDog/dd-trace-java/blob/master/internal-api/src/main/java/datadog/trace/api/Platform.java
public class VersionUtils {
    private static final Version JAVA_VERSION =
            parseJavaVersion(System.getProperty("java.version"));

    private static Version parseJavaVersion(String javaVersion) {
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
                minor = nums.get(2);
                update = nums.get(3);
            } else {
                minor = nums.get(1);
                update = nums.get(2);
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
        public final int major;
        public final int minor;
        public final int update;

        public Version(int major, int minor, int update) {
            this.major = major;
            this.minor = minor;
            this.update = update;
        }

        public boolean is(int major) {
            return this.major == major;
        }

        public boolean is(int major, int minor) {
            return this.major == major && this.minor == minor;
        }

        public boolean is(int major, int minor, int update) {
            return this.major == major && this.minor == minor && this.update == update;
        }

        public boolean isAtLeast(int major, int minor, int update) {
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

    public static boolean isJavaVersionAtLeast(int major) {
        return isJavaVersionAtLeast(major, 0, 0);
    }

    public static boolean isJavaVersionAtLeast(int major, int minor) {
        return isJavaVersionAtLeast(major, minor, 0);
    }

    public static boolean isJavaVersionAtLeast(int major, int minor, int update) {
        return JAVA_VERSION.isAtLeast(major, minor, update);
    }
}
