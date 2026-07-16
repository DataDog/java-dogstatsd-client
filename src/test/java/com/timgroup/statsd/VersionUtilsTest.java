package com.timgroup.statsd;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class VersionUtilsTest {
    @Test
    public void parsesLegacyJavaVersions() {
        assertVersion("1.8.0_382", 8, 0, 382);
        assertVersion("1.8.0", 8, 0, 0);
        assertVersion("1.7", 7, 0, 0);
    }

    @Test
    public void parsesModernJavaVersions() {
        assertVersion("16", 16, 0, 0);
        assertVersion("17.0", 17, 0, 0);
        assertVersion("17.0.10+7", 17, 0, 10);
        assertVersion("21-ea", 21, 0, 0);
    }

    @Test
    public void rejectsUnrecognizedJavaVersions() {
        assertVersion("not-a-version", 0, 0, 0);
    }

    @Test
    public void comparesVersions() {
        VersionUtils.Version version = VersionUtils.parseJavaVersion("17.0.10");

        assertEquals(true, version.isAtLeast(17, 0, 10));
        assertEquals(true, version.isAtLeast(16, 0, 0));
        assertEquals(false, version.isAtLeast(17, 0, 11));
    }

    private static void assertVersion(
            String input, int expectedMajor, int expectedMinor, int expectedUpdate) {
        VersionUtils.Version version = VersionUtils.parseJavaVersion(input);

        assertEquals(expectedMajor, version.major);
        assertEquals(expectedMinor, version.minor);
        assertEquals(expectedUpdate, version.update);
    }
}
