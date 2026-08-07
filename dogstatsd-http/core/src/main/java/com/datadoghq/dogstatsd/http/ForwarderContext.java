/* Unless explicitly stated otherwise all files in this repository are
 * licensed under the Apache 2.0 License.
 *
 * This product includes software developed at Datadog
 *  (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.
 */

package com.datadoghq.dogstatsd.http;

import java.net.URI;
import java.util.Map;

/** Provides common parameters to the forwarder implementations. */
public class ForwarderContext {
    private final URI baseUri;
    private final String localData;
    private final String externalData;

    private ForwarderContext(final URI baseUri, final String localData, final String externalData) {
        this.baseUri = baseUri;
        this.localData = localData;
        this.externalData = externalData;
    }

    /**
     * Creates a builder for a context with explicit values or detection disabled.
     *
     * @return a new builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    public URI baseUri() {
        return baseUri;
    }

    /**
     * Returns the local-data value: a container ID, an {@code in-<inode>} cgroup fallback, or null
     * when neither could be determined.
     *
     * @return the local-data value, or null.
     */
    public String localData() {
        return localData;
    }

    /**
     * Returns the external-data value from {@code DD_EXTERNAL_ENV}, or null when it is unset.
     *
     * @return the external-data value, or null.
     */
    public String externalData() {
        return externalData;
    }

    /**
     * Returns new instance with default settings.
     *
     * @return new default instance.
     */
    public static ForwarderContext defaults() {
        return builder().build();
    }

    /** Builds a {@link ForwarderContext}. Obtained via {@link ForwarderContext#builder}. */
    public static final class Builder {
        private Boolean originDetectionEnabled = null;
        private EnvMap env = new EnvMap();
        private CgroupReader cgroupReader = new CgroupReader();
        private String localData;
        private String externalData;
        private String baseUri;

        private Builder() {}

        /**
         * Sets the local-data value explicitly, skipping detection for it.
         *
         * @param val the local-data value, or null to detect it.
         * @return this builder.
         */
        public Builder localData(final String val) {
            localData = val;
            return this;
        }

        /**
         * Sets the external-data value explicitly, skipping detection for it.
         *
         * @param val the external-data value, or null to detect it.
         * @return this builder.
         */
        public Builder externalData(final String val) {
            externalData = val;
            return this;
        }

        /**
         * Enables or disables detection of values that were not set explicitly. Defaults to the
         * {@code DD_ORIGIN_DETECTION_ENABLED} environment variable, and to true when that is unset.
         *
         * @param val whether to detect local and external data.
         * @return this builder.
         */
        public Builder originDetectionEnabled(final boolean val) {
            originDetectionEnabled = val;
            return this;
        }

        /**
         * Sets the base URI the series and sketches endpoints are resolved against. Defaults to the
         * value of the {@code DD_DOGSTATSD_HTTP_URL} environment variable.
         *
         * @param val the base URI, or null to use the default.
         * @return this builder.
         */
        public Builder baseUri(final String uri) {
            baseUri = uri;
            return this;
        }

        public Builder environment(final Map<String, String> val) {
            env = new EnvMap(val);
            return this;
        }

        Builder cgroupReader(final CgroupReader val) {
            cgroupReader = val;
            return this;
        }

        /**
         * Builds the context, running detection for any value not set explicitly.
         *
         * @return a new context.
         * @throws URISyntaxException if baseUri value is not a valid URI.
         */
        public ForwarderContext build() {
            String local = localData;
            String external = externalData;

            if (resolveOriginDetectionEnabled()) {
                if (local == null) {
                    local = cgroupReader.getContainerID();
                }
                if (external == null) {
                    external = env.get("DD_EXTERNAL_ENV");
                }
            }

            return new ForwarderContext(resolveBaseUri(), local, external);
        }

        boolean resolveOriginDetectionEnabled() {
            if (originDetectionEnabled != null) {
                return originDetectionEnabled;
            }

            final String value = env.get("DD_ORIGIN_DETECTION_ENABLED");
            if (value == null) {
                return true;
            }
            final String normalized = value.trim().toLowerCase();
            return !("no".equals(normalized)
                    || "false".equals(normalized)
                    || "0".equals(normalized)
                    || "n".equals(normalized)
                    || "off".equals(normalized));
        }

        URI resolveBaseUri() {
            if (baseUri == null) {
                baseUri = env.get("DD_DOGSTATSD_HTTP_URL");
            }
            if (baseUri == null) {
                throw new IllegalStateException(
                        "baseUri is not set and DD_DOGSTATSD_HTTP_URL is not defined");
            }
            // Make sure baseUri acts as a prefix when we use it with URI#resolve later.
            if (!baseUri.endsWith("/")) {
                baseUri += "/";
            }
            return URI.create(baseUri);
        }
    }
}
