/* Unless explicitly stated otherwise all files in this repository are
 * licensed under the Apache 2.0 License.
 *
 * This product includes software developed at Datadog
 *  (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.
 */

package com.datadoghq.dogstatsd.http;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class ForwarderContextTest {
    /** A reader that reports a fixed container ID instead of reading /proc. */
    private static class StubCgroupReader extends CgroupReader {
        private final String containerID;

        StubCgroupReader(String containerID) {
            this.containerID = containerID;
        }

        @Override
        public String getContainerID() {
            return containerID;
        }
    }

    private static ForwarderContext.Builder builder(Map<String, String> env, String containerID) {
        return ForwarderContext.builder()
                .environment(env)
                .cgroupReader(new StubCgroupReader(containerID));
    }

    @Test
    public void detectionFillsBothValues() {
        Map<String, String> env = new HashMap<String, String>();
        env.put("DD_EXTERNAL_ENV", "en-xyz");

        ForwarderContext ctx = builder(env, "container-id").build();
        assertEquals("container-id", ctx.localData());
        assertEquals("en-xyz", ctx.externalData());
    }

    /** The cgroup reader supplies local data; nothing else does. */
    @Test
    public void cgroupReaderSuppliesLocalData() {
        ForwarderContext ctx = builder(new HashMap<String, String>(), "in-1234567").build();
        assertEquals("in-1234567", ctx.localData());
        assertNull(ctx.externalData());
    }

    @Test
    public void nullContainerIdLeavesLocalDataUnset() {
        assertNull(builder(new HashMap<String, String>(), null).build().localData());
    }

    @Test
    public void unsetExternalEnvLeavesExternalDataUnset() {
        assertNull(builder(new HashMap<String, String>(), "container-id").build().externalData());
    }

    /** Explicit values take precedence over anything detection would find. */
    @Test
    public void explicitValuesSkipDetection() {
        Map<String, String> env = new HashMap<String, String>();
        env.put("DD_EXTERNAL_ENV", "detected-en");

        ForwarderContext ctx =
                builder(env, "detected-ci")
                        .localData("explicit-ci")
                        .externalData("explicit-en")
                        .build();
        assertEquals("explicit-ci", ctx.localData());
        assertEquals("explicit-en", ctx.externalData());
    }

    /** Detection still runs for the value that was not set explicitly. */
    @Test
    public void explicitLocalDataStillDetectsExternalData() {
        Map<String, String> env = new HashMap<String, String>();
        env.put("DD_EXTERNAL_ENV", "en-xyz");

        ForwarderContext ctx = builder(env, "detected-ci").localData("explicit-ci").build();
        assertEquals("explicit-ci", ctx.localData());
        assertEquals("en-xyz", ctx.externalData());
    }

    /**
     * One {@code DD_ORIGIN_DETECTION_ENABLED} value, and whether it leaves origin detection
     * enabled. A null value means the variable is not set at all.
     */
    private static class Case {
        final String value;
        final boolean detects;

        Case(String value, boolean detects) {
            this.value = value;
            this.detects = detects;
        }

        @Override
        public String toString() {
            return "DD_ORIGIN_DETECTION_ENABLED="
                    + (value == null ? "<unset>" : "[" + value + "]")
                    + " should "
                    + (detects ? "detect" : "not detect");
        }
    }

    private static Case detects(String value) {
        return new Case(value, true);
    }

    private static Case ignores(String value) {
        return new Case(value, false);
    }

    /**
     * The accepted values and their meanings match {@code
     * NonBlockingStatsDClient.isOriginDetectionEnabled}: only an explicitly falsy value disables
     * detection, and everything else leaves it enabled.
     */
    private static final Case[] ORIGIN_DETECTION_CASES = {
        detects(null),
        detects(""),
        detects(" "),
        detects("   "),
        detects("\t"),
        detects("\n"),
        ignores("no"),
        ignores("false"),
        ignores("0"),
        ignores("n"),
        ignores("off"),
        ignores("NO"),
        ignores("False"),
        ignores("N"),
        ignores("OFF"),
        ignores("oFf"),
        ignores(" no"),
        ignores("false "),
        ignores("  0  "),
        ignores("\toff\t"),
        ignores("\n false \n"),
        detects("yes"),
        detects("true"),
        detects("1"),
        detects("y"),
        detects("on"),
        detects("YES"),
        detects("True"),
        detects("unknown"),
    };

    @Test
    public void originDetectionEnabledEnvVar() {
        for (Case c : ORIGIN_DETECTION_CASES) {
            Map<String, String> env = new HashMap<String, String>();
            if (c.value != null) {
                env.put("DD_ORIGIN_DETECTION_ENABLED", c.value);
            }

            boolean detects =
                    ForwarderContext.builder().environment(env).resolveOriginDetectionEnabled();
            assertEquals(c.value, c.detects, detects);
        }
    }

    /** Disabling detection skips the cgroup read and the DD_EXTERNAL_ENV lookup alike. */
    @Test
    public void disabledOriginDetectionSkipsBothLookups() {
        Map<String, String> env = new HashMap<String, String>();
        env.put("DD_ORIGIN_DETECTION_ENABLED", "false");
        env.put("DD_EXTERNAL_ENV", "en-xyz");

        ForwarderContext ctx = builder(env, "container-id").build();
        assertNull(ctx.localData());
        assertNull(ctx.externalData());
    }

    /** Values set explicitly on the builder are kept even with detection turned off. */
    @Test
    public void disabledOriginDetectionKeepsExplicitValues() {
        Map<String, String> env = new HashMap<String, String>();
        env.put("DD_ORIGIN_DETECTION_ENABLED", "false");

        ForwarderContext ctx =
                builder(env, "detected-ci")
                        .localData("explicit-ci")
                        .externalData("explicit-en")
                        .build();
        assertEquals("explicit-ci", ctx.localData());
        assertEquals("explicit-en", ctx.externalData());
    }

    @Test
    public void builderOverridesEnvironmentWhenDisabling() {
        Map<String, String> env = new HashMap<String, String>();
        env.put("DD_ORIGIN_DETECTION_ENABLED", "true");
        env.put("DD_EXTERNAL_ENV", "en-xyz");

        ForwarderContext ctx = builder(env, "container-id").originDetectionEnabled(false).build();
        assertNull(ctx.localData());
        assertNull(ctx.externalData());
    }

    @Test
    public void builderOverridesEnvironmentWhenEnabling() {
        Map<String, String> env = new HashMap<String, String>();
        env.put("DD_ORIGIN_DETECTION_ENABLED", "false");
        env.put("DD_EXTERNAL_ENV", "en-xyz");

        ForwarderContext ctx = builder(env, "container-id").originDetectionEnabled(true).build();
        assertEquals("container-id", ctx.localData());
        assertEquals("en-xyz", ctx.externalData());
    }

    /** An empty DD_EXTERNAL_ENV is passed through as-is rather than treated as unset. */
    @Test
    public void emptyExternalEnvIsPassedThrough() {
        Map<String, String> env = new HashMap<String, String>();
        env.put("DD_EXTERNAL_ENV", "");

        ForwarderContext ctx = builder(env, "container-id").build();
        assertEquals("", ctx.externalData());
    }

    @Test
    public void emptyEnvironmentDetectsLocalDataOnly() {
        ForwarderContext ctx =
                ForwarderContext.builder()
                        .environment(Collections.<String, String>emptyMap())
                        .cgroupReader(new StubCgroupReader("container-id"))
                        .build();
        assertEquals("container-id", ctx.localData());
        assertNull(ctx.externalData());
    }
}
